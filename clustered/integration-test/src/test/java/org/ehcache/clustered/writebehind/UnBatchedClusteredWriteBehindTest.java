package org.ehcache.clustered.writebehind;

import org.ehcache.config.builders.WriteBehindConfigurationBuilder;

public class UnBatchedClusteredWriteBehindTest extends AbstractClusteredWriteBehindTest {
  @Override
  WriteBehindConfigurationBuilder getWriteBehindConfiguration() {
    return WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration();
  }
}
