package com.linkedin.datahub.upgrade.system.aliases;

import static com.linkedin.datahub.upgrade.system.AbstractMCLStep.LAST_URN_KEY;
import static com.linkedin.metadata.Constants.ALIASES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.datahub.util.RecordUtils;
import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.Aliases;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.AliasesUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Backfills the system-owned {@code aliases} aspect ({@code lowercasedUrn}) for datasets created
 * before {@code AliasesSideEffect} shipped; the side effect fires only on the dataset key aspect,
 * which is written once at creation.
 *
 * <p>The SUCCEEDED marker on {@code urn:li:dataHubUpgrade:dataset-aliases-v1} gates
 * case-insensitive resolution, so it is written only when the urn-ordered scan runs out of rows —
 * never off a row count, and never when a configured limit truncated the scan.
 */
@Slf4j
public class BackfillDatasetAliasesStep implements UpgradeStep {

  /** The {@code -vN} suffix is the lowercasing-rule version; bump it when the rule changes. */
  public static final String UPGRADE_ID = AliasesUtils.DATASET_ALIASES_BACKFILL_UPGRADE_ID;

  private static final String DATASET_URN_LIKE = "urn:li:" + DATASET_ENTITY_NAME + ":%";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;
  private final int batchSize;
  private final long batchDelayMs;
  private final int limit;
  private final boolean reprocessEnabled;

  public BackfillDatasetAliasesStep(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull AspectDao aspectDao,
      int batchSize,
      long batchDelayMs,
      int limit,
      boolean reprocessEnabled) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.batchSize = batchSize;
    this.batchDelayMs = batchDelayMs;
    this.limit = limit;
    this.reprocessEnabled = reprocessEnabled;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  protected Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  @Override
  public int retryCount() {
    return 3;
  }

  @Override
  public boolean isOptional() {
    return false;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (!aspectDao.supportsRestoreIndicesScan()) {
      // Cassandra: the scan is an empty stub, so skip without writing any marker.
      log.warn(
          "{}: primary store does not support RestoreIndices scans (Cassandra); "
              + "dataset aliases backfill will not run and case-insensitive URN resolution "
              + "will not activate.",
          id());
      return true;
    }

    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

    boolean previousRunFinal =
        prevResult
            .filter(
                result ->
                    DataHubUpgradeState.SUCCEEDED.equals(result.getState())
                        || DataHubUpgradeState.ABORTED.equals(result.getState()))
            .isPresent();

    if (previousRunFinal && reprocessEnabled) {
      log.info("{}: reprocess enabled, ignoring previous final state and re-running.", id());
      return false;
    }

    if (previousRunFinal) {
      log.info(
          "{} was already run. State: {} Skipping.",
          id(),
          prevResult.map(DataHubUpgradeResult::getState));
    }
    return previousRunFinal;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      Optional<DataHubUpgradeResult> prevResult =
          context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);
      Optional<Map<String, String>> resumeState =
          prevResult
              .filter(
                  result ->
                      DataHubUpgradeState.IN_PROGRESS.equals(result.getState())
                          && result.getResult() != null)
              .map(DataHubUpgradeResult::getResult);

      String lastUrn = resumeState.map(state -> state.getOrDefault(LAST_URN_KEY, "")).orElse("");
      // A crashed run may have committed rows whose MCL never reached Kafka; they read back as
      // matched and a rewrite is no-op'd, so a resume re-emits MCLs for all matched rows.
      boolean repairing = resumeState.isPresent();
      if (repairing) {
        log.info(
            "{}: Resuming from URN: {}, re-emitting MCLs for matched rows",
            getUpgradeIdUrn(),
            lastUrn.isEmpty() ? "<start>" : lastUrn);
      }

      RunStats stats = new RunStats();

      long totalRows = 0;
      boolean stoppedByLimit = false;
      while (true) {
        List<EbeanAspectV2> page = fetchPage(context.opContext(), lastUrn);
        if (page.isEmpty()) {
          break;
        }
        String pageEndUrn = page.get(page.size() - 1).getUrn();

        // Checkpoint before writing: a crash mid-page leaves an IN_PROGRESS row, so the resume
        // re-reads this page in repair mode.
        saveCheckpoint(context, lastUrn);

        processPage(page, repairing, stats);
        totalRows += page.size();

        if (pageEndUrn.equals(lastUrn)) {
          // Only rows tying with the inclusive boundary. A short page means the scan is done; a
          // full page means the cursor cannot advance, so fail rather than spin or claim coverage.
          if (page.size() < batchSize) {
            break;
          }
          throw new IllegalStateException(
              String.format(
                  "%s: scan cannot advance past urn %s; all %d rows of the page tie with it. "
                      + "Raise batchSize (currently %d).",
                  id(), lastUrn, page.size(), batchSize));
        }
        lastUrn = pageEndUrn;
        if (limit > 0 && totalRows >= limit) {
          stoppedByLimit = true;
          break;
        }
        if (page.size() < batchSize) {
          break;
        }
        sleep();
      }

      if (stoppedByLimit) {
        // Deliberately partial: keep the IN_PROGRESS checkpoint, never write the marker.
        saveCheckpoint(context, lastUrn);
        log.warn(
            "{}: stopped after {} rows due to configured limit ({}); coverage is partial and no "
                + "completion marker was written.",
            id(),
            totalRows,
            limit);
        context
            .report()
            .addLine(
                String.format(
                    "%s: partial run (limit=%d), no completion marker. %s", id(), limit, stats));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      }

      context
          .upgrade()
          .setUpgradeResult(
              opContext, getUpgradeIdUrn(), entityService, DataHubUpgradeState.SUCCEEDED, null);
      log.info("{}: completed. {}", id(), stats);
      context.report().addLine(String.format("%s: completed. %s", id(), stats));

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  /**
   * One keyset query per page ({@code limit == batchSize}), cursor closed before processing. The
   * boundary is inclusive: an exclusive one needs {@code lastAspect}, whose {@code urn != lastUrn}
   * term is collation-sensitive and silently drops case-variant urns on a case-insensitive table.
   */
  private List<EbeanAspectV2> fetchPage(OperationContext scanOpContext, String lastUrn) {
    RestoreIndicesArgs args =
        new RestoreIndicesArgs()
            .aspectName(DATASET_KEY_ASPECT_NAME)
            .urnLike(DATASET_URN_LIKE)
            .batchSize(batchSize)
            .limit(batchSize)
            .urnBasedPagination(true)
            .lastUrn(lastUrn);
    try (PartitionedStream<EbeanAspectV2> stream =
        aspectDao.streamAspectBatches(scanOpContext, args)) {
      return stream.partition(batchSize).flatMap(Function.identity()).collect(Collectors.toList());
    }
  }

  /**
   * Classify a page (missing / matched / mismatched), ingest the write set, and block until every
   * MCL is confirmed produced to Kafka.
   *
   * @param repairMode re-emit MCLs even for matched rows (resumed runs)
   */
  private void processPage(List<EbeanAspectV2> page, boolean repairMode, RunStats stats) {
    Map<String, DatasetUrn> expectedByUrn = new HashMap<>();
    for (EbeanAspectV2 row : page) {
      String rawUrn = row.getUrn();
      try {
        expectedByUrn.put(rawUrn, AliasesUtils.lowercaseDatasetUrn(Urn.createFromString(rawUrn)));
      } catch (URISyntaxException e) {
        // mirrors AliasesSideEffect: such urns can never carry the aspect
        log.warn("{}: skipping unparseable dataset urn {}", id(), rawUrn);
        stats.unparseable++;
      }
    }

    Set<EntityAspectIdentifier> keys =
        expectedByUrn.keySet().stream()
            .map(urn -> new EntityAspectIdentifier(urn, ALIASES_ASPECT_NAME, ASPECT_LATEST_VERSION))
            .collect(Collectors.toSet());
    Map<EntityAspectIdentifier, EntityAspect> existing =
        keys.isEmpty() ? Map.of() : aspectDao.batchGet(opContext, keys, false);

    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME);
    AspectSpec aspectSpec = entitySpec.getAspectSpec(ALIASES_ASPECT_NAME);

    List<ChangeItemImpl> toWrite = new ArrayList<>();
    List<Future<?>> futures = new ArrayList<>();

    for (Map.Entry<String, DatasetUrn> entry : expectedByUrn.entrySet()) {
      String rawUrn = entry.getKey();
      DatasetUrn expected = entry.getValue();
      EntityAspect stored =
          existing.get(
              new EntityAspectIdentifier(rawUrn, ALIASES_ASPECT_NAME, ASPECT_LATEST_VERSION));

      Aliases storedAliases = parseStoredAliases(stored, rawUrn);
      boolean matched =
          storedAliases != null
              && storedAliases.hasLowercasedUrn()
              && expected.toString().equals(storedAliases.getLowercasedUrn().toString());

      if (matched) {
        stats.matched++;
        if (repairMode) {
          SystemMetadata storedSystemMetadata =
              stored.getSystemMetadata() != null
                  ? SystemMetadataUtils.parseSystemMetadata(stored.getSystemMetadata())
                  : SystemMetadataUtils.createDefaultSystemMetadata();
          Pair<Future<?>, Boolean> future =
              entityService.alwaysProduceMCLAsync(
                  opContext,
                  parseUrnUnchecked(rawUrn),
                  DATASET_ENTITY_NAME,
                  ALIASES_ASPECT_NAME,
                  aspectSpec,
                  null,
                  storedAliases,
                  null,
                  storedSystemMetadata.setRunId(id()).setLastObserved(System.currentTimeMillis()),
                  AuditStampUtils.createDefaultAuditStamp(),
                  ChangeType.UPSERT);
          futures.add(future.getFirst());
          stats.repaired++;
        }
        continue;
      }

      if (storedAliases != null) {
        stats.mismatched++;
      }
      toWrite.add(
          ChangeItemImpl.builder()
              .changeType(ChangeType.UPSERT)
              .urn(parseUrnUnchecked(rawUrn))
              .entitySpec(entitySpec)
              .aspectName(ALIASES_ASPECT_NAME)
              .aspectSpec(aspectSpec)
              .recordTemplate(new Aliases().setLowercasedUrn(expected))
              .auditStamp(AuditStampUtils.createDefaultAuditStamp())
              .systemMetadata(backfillSystemMetadata())
              .build(opContext.getAspectRetriever()));
    }

    if (!toWrite.isEmpty()) {
      AspectsBatch batch =
          AspectsBatchImpl.builder()
              .retrieverContext(opContext.getRetrieverContext())
              .items(toWrite)
              .build(opContext);
      List<UpdateAspectResult> results = entityService.ingestAspects(opContext, batch, true, true);
      if (results.size() < toWrite.size()) {
        // Without a request context, validation failures are logged and dropped instead of thrown;
        // a dropped row must fail the page, or the marker would falsely cover it.
        throw new IllegalStateException(
            String.format(
                "%s: %d of %d writes dropped by validation",
                id(), toWrite.size() - results.size(), toWrite.size()));
      }
      for (UpdateAspectResult result : results) {
        if (result.getMclFuture() != null) {
          futures.add(result.getMclFuture());
        }
      }
      stats.written += toWrite.size();
    }

    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(String.format("%s: MCL production not confirmed", id()), e);
      } catch (ExecutionException e) {
        throw new RuntimeException(String.format("%s: MCL production not confirmed", id()), e);
      }
    }
  }

  /** Corrupt stored JSON is treated as missing, so the row is rewritten with the correct value. */
  @Nullable
  private Aliases parseStoredAliases(@Nullable EntityAspect stored, String urn) {
    if (stored == null || stored.getMetadata() == null) {
      return null;
    }
    try {
      return RecordUtils.toRecordTemplate(Aliases.class, stored.getMetadata());
    } catch (ModelConversionException e) {
      log.warn("{}: rewriting unreadable stored aliases for {}", id(), urn);
      return null;
    }
  }

  private void saveCheckpoint(UpgradeContext context, String lastUrn) {
    log.info("{}: Saving state. Last urn:{}", getUpgradeIdUrn(), lastUrn);
    context
        .upgrade()
        .setUpgradeResult(
            opContext,
            getUpgradeIdUrn(),
            entityService,
            DataHubUpgradeState.IN_PROGRESS,
            Map.of(LAST_URN_KEY, lastUrn));
  }

  private SystemMetadata backfillSystemMetadata() {
    SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata(id());
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);
    systemMetadata.setProperties(properties);
    return systemMetadata;
  }

  private static Urn parseUrnUnchecked(String rawUrn) {
    try {
      return Urn.createFromString(rawUrn);
    } catch (URISyntaxException e) {
      // unreachable: rawUrn already parsed once during classification
      throw new IllegalStateException(e);
    }
  }

  private void sleep() {
    if (batchDelayMs > 0) {
      try {
        Thread.sleep(batchDelayMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private static final class RunStats {
    private long written;
    private long matched;
    private long mismatched;
    private long repaired;
    private long unparseable;

    @Override
    public String toString() {
      return String.format(
          "written=%d (mismatched=%d), matched-skipped=%d, mcl-repaired=%d, unparseable=%d",
          written, mismatched, matched, repaired, unparseable);
    }
  }
}
