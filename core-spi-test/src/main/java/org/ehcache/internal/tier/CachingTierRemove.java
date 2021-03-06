/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.internal.tier;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the {@link CachingTier#invalidate(Object)} contract of the
 * {@link CachingTier CachingTier} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */
public class CachingTierRemove<K, V> extends CachingTierTester<K, V> {

  private CachingTier tier;

  public CachingTierRemove(final CachingTierFactory<K, V> factory) {
    super(factory);
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    if (tier != null) {
//      tier.close();
      tier = null;
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void removeMapping() throws LegalSPITesterException {
    K key = factory.createKey(1);

    V originalValue = factory.createValue(1);
    V newValue = factory.createValue(2);

    final Store.ValueHolder<V> valueHolder = mock(Store.ValueHolder.class);
    when(valueHolder.value()).thenReturn(originalValue);

    tier = factory.newCachingTier(factory.newConfiguration(factory.getKeyType(), factory.getValueType(),
        1L, null, null, Expirations.noExpiration()));

    try {
      tier.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
        @Override
        public Store.ValueHolder<V> apply(final K k) {
          return valueHolder;
        }
      });

      tier.invalidate(key);

      final Store.ValueHolder<V> newValueHolder = mock(Store.ValueHolder.class);
      when(newValueHolder.value()).thenReturn(newValue);
      Store.ValueHolder<V> newReturnedValueHolder = tier.getOrComputeIfAbsent(key, new Function() {
        @Override
        public Object apply(final Object o) {
          return newValueHolder;
        }
      });

      assertThat(newReturnedValueHolder.value(), is(equalTo(newValueHolder.value())));
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }
}
