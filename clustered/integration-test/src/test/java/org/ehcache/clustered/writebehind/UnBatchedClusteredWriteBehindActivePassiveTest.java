package org.ehcache.clustered.writebehind;

import org.ehcache.config.builders.WriteBehindConfigurationBuilder;

import java.util.concurrent.TimeUnit;

public class UnBatchedClusteredWriteBehindActivePassiveTest extends AbstractClusteredWriteBehindActivePassiveTest {

  @Override
  WriteBehindConfigurationBuilder getWriteBehindConfiguration() {
    return WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration();
  }
}
