package com.linkedin.datahub.upgrade.system.aliases;

import static com.linkedin.metadata.Constants.ALIASES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.DATASET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Aliases;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BackfillDatasetAliasesStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  // The rule lowercases the dataset name only; platform and env casing are preserved.
  private static final String URN_MISSING =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,db.AAA,PROD)";
  private static final String URN_MATCHED =
      "urn:li:dataset:(urn:li:dataPlatform:MySQL,db.BBB,PROD)";
  private static final String URN_MATCHED_LOWER =
      "urn:li:dataset:(urn:li:dataPlatform:MySQL,db.bbb,PROD)";
  private static final String URN_MISMATCHED =
      "urn:li:dataset:(urn:li:dataPlatform:Snowflake,DB.CCC,PROD)";
  // A value an earlier rule version would have produced: platform lowercased too. Stored rows
  // like this must be detected as stale and rewritten.
  private static final String URN_MISMATCHED_STALE =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.ccc,PROD)";
  private static final String URN_MISMATCHED_LOWER =
      "urn:li:dataset:(urn:li:dataPlatform:Snowflake,db.ccc,PROD)";

  private EntityService<?> mockEntityService;
  private AspectDao mockAspectDao;
  private Upgrade mockUpgrade;
  private UpgradeContext mockContext;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockAspectDao = mock(AspectDao.class);
    mockUpgrade = mock(Upgrade.class);
    mockContext = mock(UpgradeContext.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mock(UpgradeReport.class));
    when(mockContext.opContext()).thenReturn(OP_CONTEXT);
    when(mockAspectDao.supportsRestoreIndicesScan()).thenReturn(true);
    when(mockUpgrade.getUpgradeResult(any(OperationContext.class), any(Urn.class), any()))
        .thenReturn(Optional.empty());
    when(mockAspectDao.batchGet(any(OperationContext.class), any(), eq(false)))
        .thenReturn(Map.of());
    UpdateAspectResult okResult = mock(UpdateAspectResult.class);
    when(okResult.getMclFuture()).thenReturn(CompletableFuture.completedFuture(null));
    when(mockEntityService.ingestAspects(
            any(OperationContext.class), any(AspectsBatch.class), eq(true), eq(true)))
        .thenReturn(List.of(okResult));
  }

  private BackfillDatasetAliasesStep buildStep(int batchSize, int limit, boolean reprocessEnabled) {
    return new BackfillDatasetAliasesStep(
        OP_CONTEXT, mockEntityService, mockAspectDao, batchSize, 0, limit, reprocessEnabled);
  }

  private static EbeanAspectV2 keyRow(String urn) {
    return new EbeanAspectV2(
        urn,
        DATASET_KEY_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{}",
        new java.sql.Timestamp(System.currentTimeMillis()),
        "urn:li:corpuser:__datahub_system",
        null,
        null);
  }

  private static PartitionedStream<EbeanAspectV2> page(EbeanAspectV2... rows) {
    return PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.of(rows)).build();
  }

  private static EntityAspect aliasesRow(String urn, String lowercasedUrn) {
    return EntityAspect.builder()
        .urn(urn)
        .aspect(ALIASES_ASPECT_NAME)
        .version(ASPECT_LATEST_VERSION)
        .metadata(
            RecordUtils.toJsonString(
                new Aliases().setLowercasedUrn(UrnUtils.getUrn(lowercasedUrn))))
        .build();
  }

  private static EntityAspectIdentifier aliasesKey(String urn) {
    return new EntityAspectIdentifier(urn, ALIASES_ASPECT_NAME, ASPECT_LATEST_VERSION);
  }

  private void mockPreviousResult(DataHubUpgradeState state, Map<String, String> resultMap) {
    DataHubUpgradeResult prevResult = new DataHubUpgradeResult().setState(state);
    if (resultMap != null) {
      prevResult.setResult(new com.linkedin.data.template.StringMap(resultMap));
    }
    when(mockUpgrade.getUpgradeResult(any(OperationContext.class), any(Urn.class), any()))
        .thenReturn(Optional.of(prevResult));
  }

  // ── skeleton / skip ────────────────────────────────────────────────────────

  @Test
  public void testId() {
    assertEquals(buildStep(10, 0, false).id(), "dataset-aliases-v1");
  }

  @Test
  public void testRetryCountAndRequired() {
    BackfillDatasetAliasesStep step = buildStep(10, 0, false);
    assertEquals(step.retryCount(), 3);
    assertFalse(step.isOptional());
  }

  @Test
  public void testSkipWhenPreviousRunSucceeded() {
    mockPreviousResult(DataHubUpgradeState.SUCCEEDED, null);
    assertTrue(buildStep(10, 0, false).skip(mockContext));
  }

  @Test
  public void testSkipWhenPreviousRunAborted() {
    mockPreviousResult(DataHubUpgradeState.ABORTED, null);
    assertTrue(buildStep(10, 0, false).skip(mockContext));
  }

  @Test
  public void testNoSkipWhenPreviousRunInProgress() {
    mockPreviousResult(DataHubUpgradeState.IN_PROGRESS, Map.of("lastUrn", URN_MISSING));
    assertFalse(buildStep(10, 0, false).skip(mockContext));
  }

  @Test
  public void testNoSkipWhenNoPreviousRun() {
    assertFalse(buildStep(10, 0, false).skip(mockContext));
  }

  @Test
  public void testReprocessOverridesSucceeded() {
    mockPreviousResult(DataHubUpgradeState.SUCCEEDED, null);
    assertFalse(buildStep(10, 0, true).skip(mockContext));
  }

  @Test
  public void testSkipWithoutMarkerWhenScanUnsupported() {
    // Cassandra: streamAspectBatches is an empty stub, so the step must not run — and above
    // all must never record a completion marker it cannot vouch for.
    when(mockAspectDao.supportsRestoreIndicesScan()).thenReturn(false);
    assertTrue(buildStep(10, 0, false).skip(mockContext));
    verify(mockUpgrade, never())
        .setUpgradeResult(any(OperationContext.class), any(Urn.class), any(), any(), any());
  }

  // ── executable: classification and write ──────────────────────────────────

  @Test
  public void testWritesMissingAndMismatchedSkipsMatched() {
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(URN_MISSING), keyRow(URN_MATCHED), keyRow(URN_MISMATCHED)));
    when(mockAspectDao.batchGet(any(OperationContext.class), any(), eq(false)))
        .thenReturn(
            Map.of(
                aliasesKey(URN_MATCHED), aliasesRow(URN_MATCHED, URN_MATCHED_LOWER),
                aliasesKey(URN_MISMATCHED), aliasesRow(URN_MISMATCHED, URN_MISMATCHED_STALE)));

    UpgradeStepResult result = buildStep(10, 0, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(1))
        .ingestAspects(any(OperationContext.class), batchCaptor.capture(), eq(true), eq(true));

    Map<String, MCPItem> itemsByUrn =
        batchCaptor.getValue().getMCPItems().stream()
            .collect(Collectors.toMap(i -> i.getUrn().toString(), i -> i));
    assertEquals(itemsByUrn.size(), 2);

    MCPItem missing = itemsByUrn.get(URN_MISSING);
    assertEquals(missing.getAspectName(), ALIASES_ASPECT_NAME);
    assertEquals(
        missing.getAspect(Aliases.class).getLowercasedUrn().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.aaa,PROD)");
    assertEquals(missing.getSystemMetadata().getRunId(), "dataset-aliases-v1");
    assertEquals(missing.getSystemMetadata().getProperties().get(APP_SOURCE), SYSTEM_UPDATE_SOURCE);

    MCPItem mismatched = itemsByUrn.get(URN_MISMATCHED);
    assertEquals(
        mismatched.getAspect(Aliases.class).getLowercasedUrn().toString(), URN_MISMATCHED_LOWER);

    // matched row is never re-written and (outside resume repair) never re-emitted
    assertFalse(itemsByUrn.containsKey(URN_MATCHED));
    verify(mockEntityService, never())
        .alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

    // terminal marker: SUCCEEDED with the checkpoint dropped
    verify(mockUpgrade)
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            eq(null));
  }

  @Test
  public void testAllMatchedPerformsNoWrites() {
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(URN_MATCHED)));
    when(mockAspectDao.batchGet(any(OperationContext.class), any(), eq(false)))
        .thenReturn(Map.of(aliasesKey(URN_MATCHED), aliasesRow(URN_MATCHED, URN_MATCHED_LOWER)));

    UpgradeStepResult result = buildStep(10, 0, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockEntityService, never())
        .ingestAspects(any(OperationContext.class), any(AspectsBatch.class), eq(true), eq(true));
  }

  // ── executable: pagination, checkpointing, termination ────────────────────

  @Test
  public void testKeysetPaginationArgsAndCheckpoints() {
    EbeanAspectV2 r1 = keyRow("urn:li:dataset:(urn:li:dataPlatform:mysql,db.a,PROD)");
    EbeanAspectV2 r2 = keyRow("urn:li:dataset:(urn:li:dataPlatform:mysql,db.b,PROD)");
    EbeanAspectV2 r3 = keyRow("urn:li:dataset:(urn:li:dataPlatform:mysql,db.c,PROD)");
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(r1, r2), page(r3));

    UpgradeStepResult result = buildStep(2, 0, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao, times(2))
        .streamAspectBatches(any(OperationContext.class), argsCaptor.capture());
    List<RestoreIndicesArgs> allArgs = argsCaptor.getAllValues();

    // page 1: fresh scan
    assertEquals(allArgs.get(0).aspectName(), DATASET_KEY_ASPECT_NAME);
    assertEquals(allArgs.get(0).urnLike(), "urn:li:dataset:%");
    assertTrue(allArgs.get(0).urnBasedPagination());
    assertEquals(allArgs.get(0).batchSize(), 2);
    assertEquals(allArgs.get(0).limit(), 2); // one page per query
    assertEquals(allArgs.get(0).lastUrn(), "");
    // the scan never filters on createdon; urn is the only cursor
    assertEquals(allArgs.get(0).gePitEpochMs(), 0);

    // page 2: strictly-exclusive keyset boundary at page 1's last row
    assertEquals(allArgs.get(1).lastUrn(), "urn:li:dataset:(urn:li:dataPlatform:mysql,db.b,PROD)");
    assertEquals(allArgs.get(1).lastAspect(), DATASET_KEY_ASPECT_NAME);
    assertEquals(allArgs.get(1).gePitEpochMs(), 0);

    // one IN_PROGRESS checkpoint per completed page, carrying lastUrn
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> checkpointCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockUpgrade, times(2))
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.IN_PROGRESS),
            checkpointCaptor.capture());
    assertEquals(
        checkpointCaptor.getAllValues().get(0).get(BackfillDatasetAliasesStep.LAST_URN_KEY),
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.b,PROD)");
    assertEquals(
        checkpointCaptor.getAllValues().get(1).get(BackfillDatasetAliasesStep.LAST_URN_KEY),
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.c,PROD)");
  }

  @Test
  public void testConfigLimitStopsWithoutMarker() {
    EbeanAspectV2 r1 = keyRow("urn:li:dataset:(urn:li:dataPlatform:mysql,db.a,PROD)");
    EbeanAspectV2 r2 = keyRow("urn:li:dataset:(urn:li:dataPlatform:mysql,db.b,PROD)");
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(r1, r2));

    UpgradeStepResult result = buildStep(2, 2, false).executable().apply(mockContext);
    // The STEP result is success (the canary run did what was asked) …
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // … but the run is explicitly partial: checkpoint only, and NEVER the marker.
    verify(mockAspectDao, times(1))
        .streamAspectBatches(any(OperationContext.class), any(RestoreIndicesArgs.class));
    verify(mockUpgrade, times(1))
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.IN_PROGRESS),
            any());
    verify(mockUpgrade, never())
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            any());
  }

  // ── executable: resume + lost-MCL repair ──────────────────────────────────

  @Test
  public void testResumeRepairReemitsMclForMatchedOnFirstPageOnly() {
    String resumeUrn = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.a,PROD)";
    mockPreviousResult(
        DataHubUpgradeState.IN_PROGRESS,
        Map.of(BackfillDatasetAliasesStep.LAST_URN_KEY, resumeUrn));

    String m1 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m1,PROD)";
    String m1Lower = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m1,PROD)";
    String m2 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m2,PROD)";
    String m2Lower = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m2,PROD)";
    String m3 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m3,PROD)";
    String m3Lower = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m3,PROD)";

    // page 1 (repair window): two matched rows; page 2: one matched row, ends the scan
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(m1), keyRow(m2)), page(keyRow(m3)));
    when(mockAspectDao.batchGet(any(OperationContext.class), any(), eq(false)))
        .thenReturn(
            Map.of(
                aliasesKey(m1), aliasesRow(m1, m1Lower),
                aliasesKey(m2), aliasesRow(m2, m2Lower),
                aliasesKey(m3), aliasesRow(m3, m3Lower)));
    when(mockEntityService.alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Pair.of(CompletableFuture.completedFuture(null), true));

    UpgradeStepResult result = buildStep(2, 0, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // resume boundary honored
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao, atLeastOnce())
        .streamAspectBatches(any(OperationContext.class), argsCaptor.capture());
    assertEquals(argsCaptor.getAllValues().get(0).lastUrn(), resumeUrn);
    assertEquals(argsCaptor.getAllValues().get(0).lastAspect(), DATASET_KEY_ASPECT_NAME);

    // matched rows of the FIRST page after resume get their MCL re-emitted (SQL may have
    // committed while the Kafka produce was lost); later pages are skipped silently.
    ArgumentCaptor<Urn> urnCaptor = ArgumentCaptor.forClass(Urn.class);
    verify(mockEntityService, times(2))
        .alwaysProduceMCLAsync(
            any(OperationContext.class),
            urnCaptor.capture(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any());
    List<String> reemitted =
        urnCaptor.getAllValues().stream().map(Urn::toString).collect(Collectors.toList());
    assertTrue(reemitted.contains(m1));
    assertTrue(reemitted.contains(m2));
    assertFalse(reemitted.contains(m3));
  }

  // ── executable: failure and edge handling ─────────────────────────────────

  @Test
  public void testUnparseableUrnSkippedAndRunStillCompletes() {
    // parses as a generic Urn but not as a DatasetUrn (2-tuple)
    String bad = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.a)";
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(bad), keyRow(URN_MISSING)));

    UpgradeStepResult result = buildStep(10, 0, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(1))
        .ingestAspects(any(OperationContext.class), batchCaptor.capture(), eq(true), eq(true));
    assertEquals(batchCaptor.getValue().getMCPItems().size(), 1);
    assertEquals(batchCaptor.getValue().getMCPItems().get(0).getUrn().toString(), URN_MISSING);

    verify(mockUpgrade)
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            eq(null));
  }

  @Test
  public void testMclFailureFailsRunBeforeCheckpoint() {
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(URN_MISSING)));

    CompletableFuture<Void> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("kafka unavailable"));
    UpdateAspectResult failedResult = mock(UpdateAspectResult.class);
    org.mockito.Mockito.doReturn(failed).when(failedResult).getMclFuture();
    when(mockEntityService.ingestAspects(
            any(OperationContext.class), any(AspectsBatch.class), eq(true), eq(true)))
        .thenReturn(List.of(failedResult));

    expectThrows(
        RuntimeException.class, () -> buildStep(10, 0, false).executable().apply(mockContext));

    // MCL durability unconfirmed → the page's checkpoint must not have been written,
    // and no marker either.
    verify(mockUpgrade, never())
        .setUpgradeResult(any(OperationContext.class), any(Urn.class), any(), any(), any());
  }
}
