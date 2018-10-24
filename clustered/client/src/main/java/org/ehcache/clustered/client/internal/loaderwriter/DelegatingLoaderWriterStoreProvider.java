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
package org.ehcache.clustered.client.internal.loaderwriter;

import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.service.ClusteringService.ClusteredCacheIdentifier;
import org.ehcache.config.ResourceType;
import org.ehcache.core.internal.store.StoreSupport;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.WrapperStore;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.store.loaderwriter.LoaderWriterStoreProvider.StoreRef;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

@ServiceDependencies({CacheLoaderWriterProvider.class, ClusteringService.class})
public class DelegatingLoaderWriterStoreProvider implements WrapperStore.Provider {
  private volatile ServiceProvider<Service> serviceProvider;

  private final Map<Store<?, ?>, StoreRef<?, ?>> createdStores = new ConcurrentHashMap<>();

  @Override
  public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {

    Store.Provider underlyingStoreProvider = StoreSupport
            .selectStoreProvider(serviceProvider, storeConfig.getResourcePools().getResourceTypeSet(),
                    Arrays.asList(serviceConfigs));

    Store<K, V> store = underlyingStoreProvider.createStore(storeConfig, serviceConfigs);
    DelegatingLoaderWriterStore<K, V> loaderWriterStore = new DelegatingLoaderWriterStore<>(store);
    createdStores.put(loaderWriterStore, new StoreRef<>(store, underlyingStoreProvider));
    return loaderWriterStore;
  }

  @Override
  public void releaseStore(Store<?, ?> resource) {
    StoreRef<?, ?> storeRef = createdStores.remove(resource);
    storeRef.getUnderlyingStoreProvider().releaseStore(storeRef.getUnderlyingStore());
  }

  @Override
  public void initStore(Store<?, ?> resource) {
    StoreRef<?, ?> storeRef = createdStores.get(resource);
    storeRef.getUnderlyingStoreProvider().initStore(storeRef.getUnderlyingStore());
  }

  @Override
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?>> serviceConfigs) {
    throw new UnsupportedOperationException("Its a Wrapper store provider, does not support regular ranking");
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {
    this.serviceProvider = null;
  }

  @Override
  public int wrapperStoreRank(Collection<ServiceConfiguration<?>> serviceConfigs) {
    CacheLoaderWriterConfiguration loaderWriterConfiguration = findSingletonAmongst(CacheLoaderWriterConfiguration.class, serviceConfigs);
    ClusteredCacheIdentifier clusteredCacheIdentifier = findSingletonAmongst(ClusteredCacheIdentifier.class, serviceConfigs);
    if (clusteredCacheIdentifier != null && loaderWriterConfiguration != null) {
      return 3;
    }
    return 0;
  }
}
