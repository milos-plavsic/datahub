package com.linkedin.datahub.upgrade.system.schemafield;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GenerateSchemaFieldsFromSchemaMetadataStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)");
  private static final AuditStamp AUDIT_STAMP =
      new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(0L);

  private EntityService<?> mockEntityService;
  private AspectDao mockAspectDao;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockAspectDao = mock(AspectDao.class);
  }

  @Test
  public void testFingerprintAndIdEncodeFlags() {
    GenerateSchemaFieldsFromSchemaMetadataStep bothOff = newStep(false, false, false);
    assertEquals(bothOff.getFingerprint(), "d0-o0");
    assertEquals(bothOff.id(), "schema-field-from-schema-metadata-v2-d0-o0");

    GenerateSchemaFieldsFromSchemaMetadataStep bothOn = newStep(false, true, true);
    assertEquals(bothOn.getFingerprint(), "d1-o1");
    assertEquals(bothOn.id(), "schema-field-from-schema-metadata-v2-d1-o1");

    assertEquals(GenerateSchemaFieldsFromSchemaMetadataStep.fingerprint(true, false), "d1-o0");
    assertEquals(GenerateSchemaFieldsFromSchemaMetadataStep.fingerprint(false, true), "d0-o1");
  }

  @Test
  public void testGetUrnLike() {
    assertEquals(newStep(false, false, false).getUrnLike(), "urn:li:dataset:%");
  }

  @Test
  public void testSkipWhenReprocessEnabled() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(true, true, true);
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult succeeded = mock(DataHubUpgradeResult.class);
    when(succeeded.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(succeeded));

    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testSkipWhenFingerprintAlreadySucceeded() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, true, false);
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult succeeded = mock(DataHubUpgradeResult.class);
    when(succeeded.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(), eq(mockEntityService)))
        .thenReturn(Optional.of(succeeded));

    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testDoesNotSkipWhenNoPreviousResult() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, false, false);
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testCycleBackToPriorFingerprintSkipsWithoutReprocess() {
    // d1 SUCCEEDED → switch to d0 (runs) → switch back to d1 skips; reprocess forces d1 again.
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult succeeded = mock(DataHubUpgradeResult.class);
    when(succeeded.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    GenerateSchemaFieldsFromSchemaMetadataStep d1 = newStep(false, true, false); // d1-o0
    GenerateSchemaFieldsFromSchemaMetadataStep d0 = newStep(false, false, false); // d0-o0
    Urn d1Urn = BootstrapStep.getUpgradeUrn(d1.id());
    Urn d0Urn = BootstrapStep.getUpgradeUrn(d0.id());

    // After d1 completed: first-time disable to d0 has no result → must run
    when(mockUpgrade.getUpgradeResult(eq(OP_CONTEXT), eq(d1Urn), eq(mockEntityService)))
        .thenReturn(Optional.of(succeeded));
    when(mockUpgrade.getUpgradeResult(eq(OP_CONTEXT), eq(d0Urn), eq(mockEntityService)))
        .thenReturn(Optional.empty());
    assertFalse(d0.skip(mockContext), "d1→d0 first transition must not skip");

    // After d0 also SUCCEEDED: returning to d1 looks up d1's prior SUCCEEDED → skips
    when(mockUpgrade.getUpgradeResult(eq(OP_CONTEXT), eq(d0Urn), eq(mockEntityService)))
        .thenReturn(Optional.of(succeeded));
    assertTrue(d1.skip(mockContext), "d1→d0→d1 cycle must skip without reprocess");
    assertTrue(d0.skip(mockContext), "d0 still skips once it has SUCCEEDED");

    // Manual reprocess on d1 forces a run even after the cycle
    GenerateSchemaFieldsFromSchemaMetadataStep d1Reprocess = newStep(true, true, false);
    assertFalse(d1Reprocess.skip(mockContext), "reprocess must not skip after cycle");
  }

  @Test
  public void testBuildDisabledAspectDeletesBothFlagsOnEmitsNothing() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, true, true);
    List<MCPItem> deletes = step.buildDisabledAspectDeletes(List.of(schemaMetadataChangeItem(3)));
    assertTrue(deletes.isEmpty());
  }

  @Test
  public void testBuildDisabledAspectDeletesBothFlagsOffEmitsFieldsTimesTwo() {
    int fieldCount = 4;
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, false, false);
    List<MCPItem> deletes =
        step.buildDisabledAspectDeletes(List.of(schemaMetadataChangeItem(fieldCount)));

    assertEquals(deletes.size(), fieldCount * 2);
    assertEquals(
        deletes.stream().map(MCPItem::getAspectName).collect(Collectors.toSet()),
        Set.of(DOMAINS_ASPECT_NAME, OWNERSHIP_ASPECT_NAME));
    assertEquals(
        deletes.stream().map(MCPItem::getUrn).collect(Collectors.toSet()).size(), fieldCount);
  }

  @Test
  public void testBuildDisabledAspectDeletesDomainOffOnlyEmitsDomainDeletes() {
    int fieldCount = 3;
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, false, true);
    List<MCPItem> deletes =
        step.buildDisabledAspectDeletes(List.of(schemaMetadataChangeItem(fieldCount)));

    assertEquals(deletes.size(), fieldCount);
    assertTrue(deletes.stream().allMatch(d -> DOMAINS_ASPECT_NAME.equals(d.getAspectName())));
  }

  @Test
  public void testBuildDisabledAspectDeletesIgnoresNonSchemaMetadataItems() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, false, false);
    EntitySpec datasetSpec = OP_CONTEXT.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl statusOnly =
        ChangeItemImpl.builder()
            .changeType(ChangeType.UPSERT)
            .urn(DATASET_URN)
            .entitySpec(datasetSpec)
            .aspectName(com.linkedin.metadata.Constants.STATUS_ASPECT_NAME)
            .aspectSpec(
                datasetSpec.getAspectSpec(com.linkedin.metadata.Constants.STATUS_ASPECT_NAME))
            .recordTemplate(new com.linkedin.common.Status().setRemoved(false))
            .auditStamp(AUDIT_STAMP)
            .build(OP_CONTEXT.getAspectRetriever());

    assertTrue(step.buildDisabledAspectDeletes(List.of(statusOnly)).isEmpty());
  }

  @Test
  public void testPartitionChunksBySize() {
    List<Integer> items = List.of(1, 2, 3, 4, 5, 6, 7);
    List<List<Integer>> chunks = GenerateSchemaFieldsFromSchemaMetadataStep.partition(items, 3);
    assertEquals(chunks.size(), 3);
    assertEquals(chunks.get(0), List.of(1, 2, 3));
    assertEquals(chunks.get(1), List.of(4, 5, 6));
    assertEquals(chunks.get(2), List.of(7));
    assertTrue(GenerateSchemaFieldsFromSchemaMetadataStep.partition(List.of(), 3).isEmpty());
  }

  @Test
  public void testIngestDisabledAspectDeletesChunksByBatchSize() {
    // batchSize=5 → 12 deletes should be 3 ingestAspects calls (5+5+2)
    GenerateSchemaFieldsFromSchemaMetadataStep step =
        new GenerateSchemaFieldsFromSchemaMetadataStep(
            OP_CONTEXT,
            mockEntityService,
            mockAspectDao,
            /* batchSize */ 5,
            /* batchDelayMs */ 0,
            1000,
            false,
            false,
            false);

    List<MCPItem> deletes = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      deletes.add(mock(MCPItem.class));
    }

    step.ingestDisabledAspectDeletes(deletes);

    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(3))
        .ingestAspects(eq(OP_CONTEXT), batchCaptor.capture(), eq(true), eq(false));

    List<AspectsBatch> batches = batchCaptor.getAllValues();
    assertEquals(batches.get(0).getItems().size(), 5);
    assertEquals(batches.get(1).getItems().size(), 5);
    assertEquals(batches.get(2).getItems().size(), 2);
  }

  @Test
  public void testIngestDisabledAspectDeletesNoOpWhenEmpty() {
    GenerateSchemaFieldsFromSchemaMetadataStep step = newStep(false, false, false);
    step.ingestDisabledAspectDeletes(List.of());
    verify(mockEntityService, times(0))
        .ingestAspects(any(), any(), any(Boolean.class), any(Boolean.class));
  }

  private GenerateSchemaFieldsFromSchemaMetadataStep newStep(
      boolean reprocess, boolean domainEnabled, boolean ownershipEnabled) {
    return new GenerateSchemaFieldsFromSchemaMetadataStep(
        OP_CONTEXT,
        mockEntityService,
        mockAspectDao,
        10,
        100,
        1000,
        reprocess,
        domainEnabled,
        ownershipEnabled);
  }

  private static ChangeItemImpl schemaMetadataChangeItem(int fieldCount) {
    EntitySpec datasetSpec = OP_CONTEXT.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME);
    SchemaFieldArray fields = new SchemaFieldArray();
    for (int i = 0; i < fieldCount; i++) {
      fields.add(
          new SchemaField()
              .setFieldPath("field_" + i)
              .setNativeDataType(i % 2 == 0 ? "string" : "boolean")
              .setType(
                  new SchemaFieldDataType()
                      .setType(
                          i % 2 == 0
                              ? SchemaFieldDataType.Type.create(new StringType())
                              : SchemaFieldDataType.Type.create(new BooleanType()))));
    }
    SchemaMetadata schemaMetadata =
        new SchemaMetadata()
            .setSchemaName("test")
            .setPlatform(new DataPlatformUrn("hive"))
            .setVersion(0L)
            .setHash("")
            .setPlatformSchema(
                SchemaMetadata.PlatformSchema.create(
                    new com.linkedin.schema.OtherSchema().setRawSchema("{}")))
            .setFields(fields);

    return ChangeItemImpl.builder()
        .changeType(ChangeType.UPSERT)
        .urn(DATASET_URN)
        .entitySpec(datasetSpec)
        .aspectName(SCHEMA_METADATA_ASPECT_NAME)
        .aspectSpec(datasetSpec.getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
        .recordTemplate(schemaMetadata)
        .auditStamp(AUDIT_STAMP)
        .build(OP_CONTEXT.getAspectRetriever());
  }
}
