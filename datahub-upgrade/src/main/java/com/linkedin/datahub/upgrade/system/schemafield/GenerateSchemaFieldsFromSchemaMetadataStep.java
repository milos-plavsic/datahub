package com.linkedin.datahub.upgrade.system.schemafield;

import static com.linkedin.datahub.upgrade.system.AbstractMCLStep.LAST_URN_KEY;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

/**
 * Re-ingests dataset {@code schemaMetadata} (+ {@code status}) to trigger {@code
 * SchemaFieldSideEffect} materialization of schemaField key/aliases/status, and — when MCP
 * domain/ownership mirroring flags are on — to backfill mirrored field aspects.
 *
 * <p>Upgrade id is fingerprinted by the effective domain/ownership mirror flags so a first-time
 * change to a new flag combination automatically schedules a new run. Returning to a fingerprint
 * that already SUCCEEDED (toggling in cycles) does <strong>not</strong> re-run — use manual
 * reprocess ({@code SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_REPROCESS=true}) or
 * clear/modify the stored {@code dataHubUpgradeResult} for that upgrade URN (either is valid). When
 * a flag is off, this step also deletes the corresponding aspects from schemaFields (cleanup for
 * disable). That cleanup is <strong>not provenance-aware</strong>: it deletes the aspect on every
 * field under each scanned dataset, including aspects written directly on the schemaField — not
 * only copies produced by dataset mirroring.
 *
 * <p>Environment Variables:
 *
 * <ul>
 *   <li>{@code SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA} — skip entirely when {@code true}
 *   <li>{@code SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_REPROCESS} — manual reprocess:
 *       force re-run even if this fingerprint already SUCCEEDED (also valid for cyclic toggles back
 *       to a prior state)
 * </ul>
 */
@Slf4j
public class GenerateSchemaFieldsFromSchemaMetadataStep implements UpgradeStep {
  private static final String UPGRADE_ID_PREFIX = "schema-field-from-schema-metadata-v2";
  private static final String RESULT_DOMAIN_ENABLED_KEY = "domainEnabled";
  private static final String RESULT_OWNERSHIP_ENABLED_KEY = "ownershipEnabled";
  private static final String RESULT_FINGERPRINT_KEY = "fingerprint";

  private static final List<String> REQUIRED_ASPECTS =
      List.of(SCHEMA_METADATA_ASPECT_NAME, STATUS_ASPECT_NAME);

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;

  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;
  private final boolean reprocessEnabled;
  private final boolean domainEnabled;
  private final boolean ownershipEnabled;
  private final String fingerprint;
  private final String upgradeId;

  public GenerateSchemaFieldsFromSchemaMetadataStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit,
      boolean reprocessEnabled,
      boolean domainEnabled,
      boolean ownershipEnabled) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.batchSize = batchSize;
    this.batchDelayMs = batchDelayMs;
    this.limit = limit;
    this.reprocessEnabled = reprocessEnabled;
    this.domainEnabled = domainEnabled;
    this.ownershipEnabled = ownershipEnabled;
    this.fingerprint = fingerprint(domainEnabled, ownershipEnabled);
    this.upgradeId = UPGRADE_ID_PREFIX + "-" + fingerprint;
    log.info(
        "GenerateSchemaFieldsFromSchemaMetadataStep initialized (id={}, domainEnabled={}, ownershipEnabled={}, reprocessEnabled={})",
        upgradeId,
        domainEnabled,
        ownershipEnabled,
        reprocessEnabled);
  }

  @VisibleForTesting
  public static String fingerprint(boolean domainEnabled, boolean ownershipEnabled) {
    return "d" + (domainEnabled ? "1" : "0") + "-o" + (ownershipEnabled ? "1" : "0");
  }

  @Override
  public String id() {
    return upgradeId;
  }

  @VisibleForTesting
  @Nullable
  public String getUrnLike() {
    return "urn:li:" + DATASET_ENTITY_NAME + ":%";
  }

  @VisibleForTesting
  public String getFingerprint() {
    return fingerprint;
  }

  /**
   * Returns whether the upgrade should be skipped. Skips when this flag fingerprint already
   * SUCCEEDED/ABORTED unless {@code reprocessEnabled} is set. A first-time change to a new
   * fingerprint does not skip. Toggling flags back to a previously SUCCEEDED fingerprint does skip
   * unless manual reprocess is enabled or the upgrade result is cleared/modified (either is valid).
   */
  public boolean skip(UpgradeContext context) {
    if (Boolean.parseBoolean(System.getenv("SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA"))) {
      log.info(
          "Environment variable SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA is set to true. Skipping.");
      return true;
    }

    if (reprocessEnabled) {
      log.info("{}: Reprocess enabled, not skipping.", getUpgradeIdUrn());
      return false;
    }

    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

    return prevResult
        .filter(
            result ->
                DataHubUpgradeState.SUCCEEDED.equals(result.getState())
                    || DataHubUpgradeState.ABORTED.equals(result.getState()))
        .isPresent();
  }

  protected Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    log.info("Starting GenerateSchemaFieldsFromSchemaMetadataStep ({})", id());
    return (context) -> {
      // Resume state
      Optional<DataHubUpgradeResult> prevResult =
          context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);
      String resumeUrn =
          prevResult
              .filter(
                  result ->
                      DataHubUpgradeState.IN_PROGRESS.equals(result.getState())
                          && result.getResult() != null
                          && result.getResult().containsKey(LAST_URN_KEY))
              .map(result -> result.getResult().get(LAST_URN_KEY))
              .orElse(null);
      if (resumeUrn != null) {
        log.info("{}: Resuming from URN: {}", getUpgradeIdUrn(), resumeUrn);
      }

      // re-using for configuring the sql scan
      RestoreIndicesArgs args =
          new RestoreIndicesArgs()
              .aspectNames(REQUIRED_ASPECTS)
              .batchSize(batchSize)
              .lastUrn(resumeUrn)
              .urnBasedPagination(resumeUrn != null)
              .limit(limit);

      if (getUrnLike() != null) {
        args = args.urnLike(getUrnLike());
      }

      try (PartitionedStream<EbeanAspectV2> stream =
          aspectDao.streamAspectBatches(context.opContext(), args)) {
        stream
            .partition(args.batchSize)
            .forEach(
                batch -> {
                  log.info("Processing batch of size {}.", batchSize);

                  List<ChangeItemImpl> changeItems =
                      batch
                          .flatMap(
                              ebeanAspectV2 ->
                                  EntityUtils.toSystemAspectFromEbeanAspects(
                                      opContext,
                                      opContext.getRetrieverContext(),
                                      Set.of(ebeanAspectV2))
                                      .stream())
                          .map(
                              systemAspect ->
                                  ChangeItemImpl.builder()
                                      .changeType(ChangeType.UPSERT)
                                      .urn(systemAspect.getUrn())
                                      .entitySpec(systemAspect.getEntitySpec())
                                      .aspectName(systemAspect.getAspectName())
                                      .aspectSpec(systemAspect.getAspectSpec())
                                      .recordTemplate(systemAspect.getRecordTemplate())
                                      .auditStamp(systemAspect.getAuditStamp())
                                      .systemMetadata(
                                          withAppSource(systemAspect.getSystemMetadata()))
                                      .build(opContext.getAspectRetriever()))
                          .collect(Collectors.toList());

                  AspectsBatch aspectsBatch =
                      AspectsBatchImpl.builder()
                          .retrieverContext(opContext.getRetrieverContext())
                          .items(changeItems)
                          .build(opContext);

                  // re-ingest the aspects to trigger side effects (key/aliases/status + enabled
                  // domain/ownership mirrors)
                  entityService.ingestAspects(opContext, aspectsBatch, true, false);

                  // When a mirror flag is off, remove leftover field aspects from prior enables.
                  // Chunk by batchSize — wide schemas can produce fields × disabled-aspects deletes
                  // per SQL batch (far larger than batchSize).
                  ingestDisabledAspectDeletes(buildDisabledAspectDeletes(changeItems));

                  // record progress
                  Urn lastUrn =
                      aspectsBatch.getItems().stream()
                          .reduce((a, b) -> b)
                          .map(ReadItem::getUrn)
                          .orElse(null);
                  if (lastUrn != null) {
                    log.info("{}: Saving state. Last urn:{}", getUpgradeIdUrn(), lastUrn);
                    Map<String, String> progress = new HashMap<>();
                    progress.put(LAST_URN_KEY, lastUrn.toString());
                    progress.put(RESULT_FINGERPRINT_KEY, fingerprint);
                    progress.put(RESULT_DOMAIN_ENABLED_KEY, Boolean.toString(domainEnabled));
                    progress.put(RESULT_OWNERSHIP_ENABLED_KEY, Boolean.toString(ownershipEnabled));
                    context
                        .upgrade()
                        .setUpgradeResult(
                            opContext,
                            getUpgradeIdUrn(),
                            entityService,
                            DataHubUpgradeState.IN_PROGRESS,
                            progress);
                  }

                  if (batchDelayMs > 0) {
                    log.info("Sleeping for {} ms", batchDelayMs);
                    try {
                      Thread.sleep(batchDelayMs);
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                });
      }

      Map<String, String> finalResult = new HashMap<>();
      finalResult.put(RESULT_FINGERPRINT_KEY, fingerprint);
      finalResult.put(RESULT_DOMAIN_ENABLED_KEY, Boolean.toString(domainEnabled));
      finalResult.put(RESULT_OWNERSHIP_ENABLED_KEY, Boolean.toString(ownershipEnabled));
      BootstrapStep.setUpgradeResult(
          opContext, getUpgradeIdUrn(), entityService, DataHubUpgradeState.SUCCEEDED, finalResult);
      context.report().addLine("State updated: " + getUpgradeIdUrn());

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  /**
   * For each schemaMetadata UPSERT in the batch, emit schemaField deletes for mirror aspects whose
   * flags are currently off. Deletes are not limited to previously mirrored copies — any existing
   * field {@code domains}/{@code ownership} aspect is removed.
   */
  @VisibleForTesting
  List<MCPItem> buildDisabledAspectDeletes(List<ChangeItemImpl> changeItems) {
    if (domainEnabled && ownershipEnabled) {
      return List.of();
    }

    EntitySpec schemaFieldSpec =
        opContext.getEntityRegistry().getEntitySpec(SCHEMA_FIELD_ENTITY_NAME);
    AspectSpec domainsSpec =
        domainEnabled ? null : schemaFieldSpec.getAspectSpec(DOMAINS_ASPECT_NAME);
    AspectSpec ownershipSpec =
        ownershipEnabled ? null : schemaFieldSpec.getAspectSpec(OWNERSHIP_ASPECT_NAME);
    if (domainsSpec == null && ownershipSpec == null) {
      return List.of();
    }

    List<MCPItem> deletes = new ArrayList<>();
    for (ChangeItemImpl item : changeItems) {
      if (!SCHEMA_METADATA_ASPECT_NAME.equals(item.getAspectName())
          || item.getRecordTemplate() == null) {
        continue;
      }
      SchemaMetadata schemaMetadata = item.getAspect(SchemaMetadata.class);
      if (schemaMetadata == null || !schemaMetadata.hasFields()) {
        continue;
      }
      for (SchemaField field : schemaMetadata.getFields()) {
        Urn fieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(item.getUrn(), field);
        if (domainsSpec != null) {
          deletes.add(
              DeleteItemImpl.builder()
                  .urn(fieldUrn)
                  .aspectName(DOMAINS_ASPECT_NAME)
                  .auditStamp(item.getAuditStamp())
                  .entitySpec(schemaFieldSpec)
                  .aspectSpec(domainsSpec)
                  .build(opContext.getAspectRetriever()));
        }
        if (ownershipSpec != null) {
          deletes.add(
              DeleteItemImpl.builder()
                  .urn(fieldUrn)
                  .aspectName(OWNERSHIP_ASPECT_NAME)
                  .auditStamp(item.getAuditStamp())
                  .entitySpec(schemaFieldSpec)
                  .aspectSpec(ownershipSpec)
                  .build(opContext.getAspectRetriever()));
        }
      }
    }
    return deletes;
  }

  /**
   * Ingests disable-cleanup deletes in chunks of at most {@code batchSize}, sleeping {@code
   * batchDelayMs} between chunks (not after the last). Keeps each {@code ingestAspects} call
   * bounded when a SQL batch of wide schemas yields fields × disabled-aspects deletes.
   */
  @VisibleForTesting
  void ingestDisabledAspectDeletes(List<MCPItem> cleanupDeletes) {
    if (cleanupDeletes.isEmpty()) {
      return;
    }
    int chunkSize = Math.max(1, batchSize);
    List<List<MCPItem>> chunks = partition(cleanupDeletes, chunkSize);
    log.info(
        "Ingesting {} schemaField aspect deletes for disabled mirror flags "
            + "(domainEnabled={}, ownershipEnabled={}) in {} chunk(s) of ≤{}",
        cleanupDeletes.size(),
        domainEnabled,
        ownershipEnabled,
        chunks.size(),
        chunkSize);
    for (int i = 0; i < chunks.size(); i++) {
      List<MCPItem> chunk = chunks.get(i);
      AspectsBatch deleteBatch =
          AspectsBatchImpl.builder()
              .retrieverContext(opContext.getRetrieverContext())
              .items(chunk)
              .build(opContext);
      entityService.ingestAspects(opContext, deleteBatch, true, false);
      if (i < chunks.size() - 1 && batchDelayMs > 0) {
        log.info("Sleeping for {} ms between cleanup delete chunks", batchDelayMs);
        try {
          Thread.sleep(batchDelayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }
  }

  @VisibleForTesting
  static <T> List<List<T>> partition(List<T> items, int size) {
    if (items.isEmpty()) {
      return List.of();
    }
    if (size < 1) {
      throw new IllegalArgumentException("partition size must be >= 1");
    }
    List<List<T>> chunks = new ArrayList<>((items.size() + size - 1) / size);
    for (int i = 0; i < items.size(); i += size) {
      chunks.add(List.copyOf(items.subList(i, Math.min(i + size, items.size()))));
    }
    return chunks;
  }

  private static SystemMetadata withAppSource(@Nullable SystemMetadata systemMetadata) {
    SystemMetadata withAppSourceSystemMetadata = null;
    try {
      withAppSourceSystemMetadata =
          systemMetadata != null
              ? new SystemMetadata(systemMetadata.copy().data())
              : new SystemMetadata();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    StringMap properties = withAppSourceSystemMetadata.getProperties();
    StringMap map = properties != null ? new StringMap(properties.data()) : new StringMap();
    map.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);

    withAppSourceSystemMetadata.setProperties(map);
    return withAppSourceSystemMetadata;
  }
}
