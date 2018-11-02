package org.ehcache.clustered.writebehind;

import org.ehcache.config.builders.WriteBehindConfigurationBuilder;

import java.util.concurrent.TimeUnit;

public class BatchedClusteredWriteBehindActivePassiveTest extends AbstractClusteredWriteBehindActivePassiveTest {

  @Override
  WriteBehindConfigurationBuilder getWriteBehindConfiguration() {
    return WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration(1000, TimeUnit.SECONDS, 10);
  }
}
