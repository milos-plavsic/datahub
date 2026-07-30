package com.linkedin.datahub.upgrade.system.aliases;

import static com.linkedin.metadata.Constants.ALIASES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.datahub.util.RecordUtils;
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
import lombok.extern.slf4j.Slf4j;

/**
 * Backfills the system-owned {@code aliases} aspect (field {@code lowercasedUrn}) for datasets that
 * existed before {@code AliasesSideEffect} shipped. The side effect only fires on the dataset key
 * aspect, which is written once at creation, so pre-existing datasets never receive the aspect;
 * case-insensitive URN resolution needs total coverage before it may trust a single-hit lookup (a
 * half-covered case collision resolves confidently to a possibly-wrong URN).
 *
 * <p>The completion marker ({@code dataHubUpgradeResult} state SUCCEEDED on {@code
 * urn:li:dataHubUpgrade:dataset-aliases-v1}) is therefore a correctness signal: it is only written
 * once the urn-ordered scan reaches natural exhaustion, never off a row count and never when a
 * configured limit truncated the scan. Consumers must gate resolution on this marker.
 *
 * <p>The marker's promise is point-in-time: datasets created after the scan passed their urn
 * position are covered by the side effect instead. A deployment whose write path still runs an
 * older image without the side effect can therefore leave gaps; {@code
 * systemUpdate.datasetAliases.reprocess.enabled} forces a full re-scan and repair for that case.
 */
@Slf4j
public class BackfillDatasetAliasesStep implements UpgradeStep {

  /**
   * The {@code -vN} suffix is the lowercasing-rule version; the constant lives next to {@link
   * AliasesUtils#lowercaseDatasetUrn} so a rule change and its version bump travel together.
   */
  public static final String UPGRADE_ID = AliasesUtils.DATASET_ALIASES_BACKFILL_UPGRADE_ID;

  /** Checkpoint key: urn boundary of the last fully confirmed page. */
  public static final String LAST_URN_KEY = "lastUrn";

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
    // Retries are cheap: each attempt resumes from the last confirmed page checkpoint.
    return 3;
  }

  @Override
  public boolean isOptional() {
    // Fail loudly; a silently-optional failure would strand an IN_PROGRESS marker unnoticed
    // while the resolver stays disabled.
    return false;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (!aspectDao.supportsRestoreIndicesScan()) {
      // Cassandra: the scan is a silent empty stub. Skip WITHOUT writing any marker so
      // case-insensitive resolution never activates on unverifiable coverage.
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
                          && result.getResult() != null
                          && result.getResult().containsKey(LAST_URN_KEY))
              .map(DataHubUpgradeResult::getResult);

      String lastUrn = resumeState.map(state -> state.get(LAST_URN_KEY)).orElse("");
      boolean resuming = !lastUrn.isEmpty();
      if (resuming) {
        log.info("{}: Resuming from URN: {}", getUpgradeIdUrn(), lastUrn);
      }

      RunStats stats = new RunStats();

      // urn-ordered keyset scan, one short-lived query per page.
      boolean repairPage = resuming;
      long totalRows = 0;
      boolean stoppedByLimit = false;
      while (true) {
        List<EbeanAspectV2> page = fetchPage(context.opContext(), lastUrn);
        if (page.isEmpty()) {
          break;
        }
        processPage(page, repairPage, stats);
        repairPage = false;
        totalRows += page.size();
        lastUrn = page.get(page.size() - 1).getUrn();
        saveCheckpoint(context, lastUrn);
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
        // Deliberately partial (canary/test runs): leave the IN_PROGRESS checkpoint, never the
        // marker — a row-capped scan must not be mistakable for total coverage.
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

      // Scan exhausted: every dataset key row present during the scan either carried a matching
      // value, had one committed + MCL-confirmed this run, or is unparseable (invisible to the
      // side effect and the resolver alike). Datasets created after the scan passed their urn
      // position rely on AliasesSideEffect; a deployment that creates datasets from pods still
      // running an older image can miss those, and the remedy is a reprocess run.
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
   * One short-lived keyset query: {@code limit == batchSize} caps the query at exactly one page,
   * and the cursor/connection is closed before the caller processes or sleeps. {@code lastAspect}
   * makes the boundary strictly exclusive: for {@code urn == lastUrn} the predicate demands {@code
   * aspect > 'datasetKey'}, which the aspect filter forbids.
   */
  private List<EbeanAspectV2> fetchPage(OperationContext scanOpContext, String lastUrn) {
    RestoreIndicesArgs args =
        new RestoreIndicesArgs()
            .aspectName(DATASET_KEY_ASPECT_NAME)
            .urnLike(DATASET_URN_LIKE)
            .batchSize(batchSize)
            .limit(batchSize)
            .urnBasedPagination(true)
            .lastUrn(lastUrn)
            .lastAspect(lastUrn.isEmpty() ? "" : DATASET_KEY_ASPECT_NAME);
    try (PartitionedStream<EbeanAspectV2> stream =
        aspectDao.streamAspectBatches(scanOpContext, args)) {
      return stream.partition(batchSize).flatMap(Function.identity()).collect(Collectors.toList());
    }
  }

  /**
   * Classify a page (missing / matched / mismatched), synchronously ingest the write set, and block
   * until every MCL is confirmed produced — the caller's checkpoint must imply Kafka durability. In
   * repair mode, matched rows get an unconditional MCL re-emission: after a crash between SQL
   * commit and Kafka produce the row looks matched but ES never heard about it, and a plain
   * re-write cannot fix that because an identical value is no-op'd with the MCL suppressed.
   */
  private void processPage(List<EbeanAspectV2> page, boolean repairMode, RunStats stats) {
    Map<String, DatasetUrn> expectedByUrn = new HashMap<>();
    for (EbeanAspectV2 row : page) {
      String rawUrn = row.getUrn();
      try {
        expectedByUrn.put(rawUrn, AliasesUtils.lowercaseDatasetUrn(Urn.createFromString(rawUrn)));
      } catch (URISyntaxException e) {
        // mirrors AliasesSideEffect: such urns can never carry the aspect and are equally
        // invisible to the resolver, so they are outside the coverage claim
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
    List<Pair<Future<?>, String>> futures = new ArrayList<>();

    for (Map.Entry<String, DatasetUrn> entry : expectedByUrn.entrySet()) {
      String rawUrn = entry.getKey();
      DatasetUrn expected = entry.getValue();
      EntityAspect stored =
          existing.get(
              new EntityAspectIdentifier(rawUrn, ALIASES_ASPECT_NAME, ASPECT_LATEST_VERSION));

      Aliases storedAliases =
          stored == null ? null : RecordUtils.toRecordTemplate(Aliases.class, stored.getMetadata());
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
          futures.add(Pair.of(future.getFirst(), rawUrn));
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
      for (UpdateAspectResult result : results) {
        if (result.getMclFuture() != null) {
          futures.add(Pair.of(result.getMclFuture(), "ingest"));
        }
      }
      stats.written += toWrite.size();
    }

    for (Pair<Future<?>, String> future : futures) {
      try {
        future.getFirst().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(
            String.format("%s: MCL production not confirmed (%s)", id(), future.getSecond()), e);
      }
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
