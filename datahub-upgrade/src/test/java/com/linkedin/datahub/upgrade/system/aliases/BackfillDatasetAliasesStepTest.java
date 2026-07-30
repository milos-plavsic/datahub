package com.linkedin.datahub.upgrade.system.aliases;

import static com.linkedin.datahub.upgrade.system.AbstractMCLStep.LAST_URN_KEY;
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
  // A value an earlier rule version would have produced; must be detected as stale and rewritten.
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
        .thenAnswer(
            invocation ->
                ((AspectsBatch) invocation.getArgument(1))
                    .getMCPItems().stream().map(item -> okResult).collect(Collectors.toList()));
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

  // ── skip ───────────────────────────────────────────────────────────────────

  @Test
  public void testSkipOnlyWhenPreviousRunFinal() {
    assertFalse(buildStep(10, 0, false).skip(mockContext));

    mockPreviousResult(DataHubUpgradeState.IN_PROGRESS, Map.of("lastUrn", URN_MISSING));
    assertFalse(buildStep(10, 0, false).skip(mockContext));

    mockPreviousResult(DataHubUpgradeState.SUCCEEDED, null);
    assertTrue(buildStep(10, 0, false).skip(mockContext));

    mockPreviousResult(DataHubUpgradeState.ABORTED, null);
    assertTrue(buildStep(10, 0, false).skip(mockContext));
  }

  @Test
  public void testReprocessOverridesSucceeded() {
    mockPreviousResult(DataHubUpgradeState.SUCCEEDED, null);
    assertFalse(buildStep(10, 0, true).skip(mockContext));
  }

  @Test
  public void testSkipWithoutMarkerWhenScanUnsupported() {
    // Cassandra: the scan is an empty stub, so never run and never write a marker.
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

    // matched row: never re-written, never re-emitted on a fresh run
    assertFalse(itemsByUrn.containsKey(URN_MATCHED));
    verify(mockEntityService, never())
        .alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

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

    assertEquals(allArgs.get(0).aspectName(), DATASET_KEY_ASPECT_NAME);
    assertEquals(allArgs.get(0).urnLike(), "urn:li:dataset:%");
    assertTrue(allArgs.get(0).urnBasedPagination());
    assertEquals(allArgs.get(0).batchSize(), 2);
    assertEquals(allArgs.get(0).limit(), 2); // one page per query
    assertEquals(allArgs.get(0).lastUrn(), "");

    // page 2 resumes at page 1's last row, inclusively; lastAspect stays unset on purpose (its
    // urn != lastUrn term is collation-sensitive)
    assertEquals(allArgs.get(1).lastUrn(), "urn:li:dataset:(urn:li:dataPlatform:mysql,db.b,PROD)");
    assertEquals(allArgs.get(1).lastAspect(), "");

    // one checkpoint per page, taken BEFORE the page is written, naming the previous page's end
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> checkpointCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockUpgrade, times(2))
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.IN_PROGRESS),
            checkpointCaptor.capture());
    assertEquals(checkpointCaptor.getAllValues().get(0).get(LAST_URN_KEY), "");
    assertEquals(
        checkpointCaptor.getAllValues().get(1).get(LAST_URN_KEY),
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.b,PROD)");
  }

  @Test
  public void testConfigLimitStopsWithoutMarker() {
    EbeanAspectV2 r1 = keyRow("urn:li:dataset:(urn:li:dataPlatform:mysql,db.a,PROD)");
    EbeanAspectV2 r2 = keyRow("urn:li:dataset:(urn:li:dataPlatform:mysql,db.b,PROD)");
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(r1, r2));

    UpgradeStepResult result = buildStep(2, 2, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // the run is explicitly partial: checkpoint only, never the marker
    verify(mockAspectDao, times(1))
        .streamAspectBatches(any(OperationContext.class), any(RestoreIndicesArgs.class));
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> checkpointCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockUpgrade, times(2))
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.IN_PROGRESS),
            checkpointCaptor.capture());
    // the handoff advances the boundary past the confirmed page
    assertEquals(
        checkpointCaptor.getAllValues().get(1).get(LAST_URN_KEY),
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.b,PROD)");
    verify(mockUpgrade, never())
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            any());
  }

  // ── executable: resume + lost-MCL repair ──────────────────────────────────

  /** urns of every matched row whose MCL was unconditionally re-emitted. */
  private List<String> captureReemittedUrns(int expectedCount) {
    ArgumentCaptor<Urn> urnCaptor = ArgumentCaptor.forClass(Urn.class);
    verify(mockEntityService, times(expectedCount))
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
    return urnCaptor.getAllValues().stream().map(Urn::toString).collect(Collectors.toList());
  }

  @Test
  public void testResumeReemitsMclForAllMatchedRows() {
    // A crashed run may have committed rows whose MCL never reached Kafka; they read back as
    // matched, so a resume must re-emit MCLs for matched rows across all remaining pages.
    String confirmedUrn = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.a,PROD)";
    String m1 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m1,PROD)";
    String m2 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m2,PROD)";
    String m3 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m3,PROD)";
    String fresh = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.m4,PROD)";

    mockPreviousResult(DataHubUpgradeState.IN_PROGRESS, Map.of(LAST_URN_KEY, confirmedUrn));

    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(m1), keyRow(m2)), page(keyRow(m3), keyRow(fresh)), page());
    when(mockAspectDao.batchGet(any(OperationContext.class), any(), eq(false)))
        .thenReturn(
            Map.of(
                aliasesKey(m1), aliasesRow(m1, m1),
                aliasesKey(m2), aliasesRow(m2, m2),
                aliasesKey(m3), aliasesRow(m3, m3)));
    when(mockEntityService.alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Pair.of(CompletableFuture.completedFuture(null), true));

    UpgradeStepResult result = buildStep(2, 0, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // the scan restarts from the confirmed boundary
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockAspectDao, atLeastOnce())
        .streamAspectBatches(any(OperationContext.class), argsCaptor.capture());
    assertEquals(argsCaptor.getAllValues().get(0).lastUrn(), confirmedUrn);

    // matched rows re-emit on every page; the row with no stored aspect takes the write path
    List<String> reemitted = captureReemittedUrns(3);
    assertTrue(reemitted.contains(m1));
    assertTrue(reemitted.contains(m2));
    assertTrue(reemitted.contains(m3));
    assertFalse(reemitted.contains(fresh));

    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(1))
        .ingestAspects(any(OperationContext.class), batchCaptor.capture(), eq(true), eq(true));
    assertEquals(batchCaptor.getValue().getMCPItems().size(), 1);
    assertEquals(batchCaptor.getValue().getMCPItems().get(0).getUrn().toString(), fresh);
  }

  // ── executable: failure and edge handling ─────────────────────────────────

  @Test
  public void testEmptyStoreStillWritesMarker() {
    // zero datasets (fresh install): the scan exhausts immediately and the marker must still be
    // written, or resolution would never activate
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page());

    UpgradeStepResult result = buildStep(10, 0, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockEntityService, never())
        .ingestAspects(any(OperationContext.class), any(AspectsBatch.class), eq(true), eq(true));
    verify(mockUpgrade)
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            eq(null));
  }

  @Test
  public void testCorruptStoredAliasesIsRewrittenNotStuck() {
    // an unreadable stored row must be rewritten, not fail the page on every retry forever
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(URN_MATCHED)));
    EntityAspect corrupt =
        EntityAspect.builder()
            .urn(URN_MATCHED)
            .aspect(ALIASES_ASPECT_NAME)
            .version(ASPECT_LATEST_VERSION)
            .metadata("{not json")
            .build();
    when(mockAspectDao.batchGet(any(OperationContext.class), any(), eq(false)))
        .thenReturn(Map.of(aliasesKey(URN_MATCHED), corrupt));

    UpgradeStepResult result = buildStep(10, 0, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(1))
        .ingestAspects(any(OperationContext.class), batchCaptor.capture(), eq(true), eq(true));
    assertEquals(batchCaptor.getValue().getMCPItems().get(0).getUrn().toString(), URN_MATCHED);
  }

  @Test
  public void testDroppedWriteFailsPageAndNoMarker() {
    // without a request context, validation failures are dropped instead of thrown; the step must
    // fail the page rather than let the marker cover a row that was never written
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(URN_MISSING)));
    when(mockEntityService.ingestAspects(
            any(OperationContext.class), any(AspectsBatch.class), eq(true), eq(true)))
        .thenReturn(List.of());

    expectThrows(
        IllegalStateException.class, () -> buildStep(10, 0, false).executable().apply(mockContext));

    verify(mockUpgrade, never())
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            any());
  }

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
  public void testStalledCursorFailsInsteadOfSpinningOrClaimingCoverage() {
    // a FULL page ending where it began means the cursor is stuck (forced here with batchSize=1)
    String stuck = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.a,PROD)";
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(stuck)), page(keyRow(stuck)));

    IllegalStateException e =
        expectThrows(
            IllegalStateException.class,
            () -> buildStep(1, 0, false).executable().apply(mockContext));
    assertTrue(e.getMessage().contains(stuck));

    verify(mockUpgrade, never())
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            any());
  }

  @Test
  public void testFinalPageOfTiedRowsEndsScanCleanly() {
    // a SHORT page ending at the boundary is the benign case: the scan is exhausted, not stuck
    String a = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.a,PROD)";
    String b = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.b,PROD)";
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class)))
        .thenReturn(page(keyRow(a), keyRow(b)), page(keyRow(b)));

    UpgradeStepResult result = buildStep(2, 0, false).executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockUpgrade)
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            eq(null));
  }

  @Test
  public void testMclFailureLeavesInProgressCheckpointAndNoMarker() {
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

    // the boundary stays behind the failed page, so the next run resumes in repair mode
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> checkpointCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockUpgrade, times(1))
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.IN_PROGRESS),
            checkpointCaptor.capture());
    assertEquals(checkpointCaptor.getValue().get(LAST_URN_KEY), "");

    // and never a completion marker off a failed page
    verify(mockUpgrade, never())
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            any());
  }
}
