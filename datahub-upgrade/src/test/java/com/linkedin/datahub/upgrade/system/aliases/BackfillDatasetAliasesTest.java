package com.linkedin.datahub.upgrade.system.aliases;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.Test;

public class BackfillDatasetAliasesTest {

  private BackfillDatasetAliases buildUpgrade(boolean enabled) {
    return new BackfillDatasetAliases(
        mock(OperationContext.class),
        mock(EntityService.class),
        mock(AspectDao.class),
        enabled,
        1000,
        5000,
        0,
        false);
  }

  @Test
  public void testEnabledRegistersStep() {
    BackfillDatasetAliases upgrade = buildUpgrade(true);
    assertEquals(upgrade.id(), "BackfillDatasetAliases");
    assertEquals(upgrade.steps().size(), 1);
    assertEquals(upgrade.steps().get(0).id(), "dataset-aliases-v1");
  }

  @Test
  public void testDisabledRegistersNoSteps() {
    assertTrue(buildUpgrade(false).steps().isEmpty());
  }
}
