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
package org.ehcache.impl.internal.store.loaderwriter;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.ehcache.core.exceptions.ExceptionFactory.newCacheLoadingException;
import static org.ehcache.core.exceptions.ExceptionFactory.newCacheWritingException;

public class BaseLoaderWriterStore<K, V> implements Store<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseLoaderWriterStore.class);
  private static final Supplier<Boolean> SUPPLY_FALSE = () -> Boolean.FALSE;

  private final Store<K, V> delegate;
  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private final boolean useLoaderInAtomics;
  private final ExpiryPolicy<? super K, ? super V> expiry;

  public BaseLoaderWriterStore(Store<K, V> delegate, CacheLoaderWriter<? super K, V> cacheLoaderWriter, boolean useLoaderInAtomics,
                               ExpiryPolicy<? super K, ? super V> expiry) {
    this.delegate = delegate;
    this.cacheLoaderWriter = cacheLoaderWriter;
    this.useLoaderInAtomics = useLoaderInAtomics;
    this.expiry = expiry;
  }

  @Override
  public ValueHolder<V> get(K key) throws StoreAccessException {
    Function<K, V> mappingFunction = k -> {
      try {
        return cacheLoaderWriter.load(k);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheLoadingException(e));
      }
    };
    return delegate.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public boolean containsKey(K key) throws StoreAccessException {
    return delegate.containsKey(key);
  }

  @Override
  public PutStatus put(K key, V value) throws StoreAccessException {
    BiFunction<K, V, V> remappingFunction = (key1, previousValue) -> {
      try {
        cacheLoaderWriter.write(key1, value);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }
      return value;
    };

    delegate.compute(key, remappingFunction);
    return Store.PutStatus.PUT;
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value) throws StoreAccessException {
    return null;
  }

  @Override
  public boolean remove(K key) throws StoreAccessException {
    boolean[] modified = { false };

    BiFunction<K, V, V> remappingFunction = (key1, previousValue) -> {
      modified[0] = (previousValue != null);

      try {
        cacheLoaderWriter.delete(key1);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }
      return null;
    };

    delegate.compute(key, remappingFunction);
    return modified[0];
  }

  @Override
  public RemoveStatus remove(K key, V value) throws StoreAccessException {
    boolean[] hitRemoved = { false, false }; // index 0 = hit, 1 = removed
    BiFunction<K, V, V> remappingFunction = (k, inCache) -> {
      inCache = loadFromLoaderWriter(key, inCache);
      if(inCache == null) {
        return null;
      }

      hitRemoved[0] = true;
      if (value.equals(inCache)) {
        try {
          cacheLoaderWriter.delete(k);
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheWritingException(e));
        }
        hitRemoved[1] = true;
        return null;
      }
      return inCache;
    };

    delegate.compute(key, remappingFunction, SUPPLY_FALSE);
    if (hitRemoved[1]) {
      return Store.RemoveStatus.REMOVED;
    }

    if (hitRemoved[0]) {
      return Store.RemoveStatus.KEY_PRESENT;
    } else {
      return Store.RemoveStatus.KEY_MISSING;
    }
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws StoreAccessException {
    @SuppressWarnings("unchecked")
    V[] old = (V[]) new Object[1];

    BiFunction<K, V, V> remappingFunction = (k, inCache) -> {
      inCache = loadFromLoaderWriter(key, inCache);
      if(inCache == null) {
        return null;
      }

      try {
        cacheLoaderWriter.write(key, value);
      } catch (Exception e) {
        throw new StorePassThroughException(newCacheWritingException(e));
      }

      old[0] = inCache;

      if (newValueAlreadyExpired(LOG, expiry, key, inCache, value)) {
        return null;
      }
      return value;
    };

    delegate.compute(key, remappingFunction);
    return new LoaderWriterValueHolder<>(old[0]);
  }

  @Override
  public ReplaceStatus replace(K key, V oldValue, V newValue) throws StoreAccessException {
    boolean[] successHit = { false, false }; // index 0 = success, 1 = hit

    BiFunction<K, V, V> remappingFunction = (k, inCache) -> {
      inCache = loadFromLoaderWriter(key, inCache);
      if(inCache == null) {
        return null;
      }

      successHit[1] = true;
      if (oldValue.equals(inCache)) {
        try {
          cacheLoaderWriter.write(key, newValue);
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheWritingException(e));
        }

        successHit[0] = true;

        if (newValueAlreadyExpired(LOG, expiry, key, oldValue, newValue)) {
          return null;
        }
        return newValue;
      }
      return inCache;
    };

    delegate.compute(key, remappingFunction, SUPPLY_FALSE);
    if (successHit[0]) {
      return Store.ReplaceStatus.HIT;
    } else {
      if (successHit[1]) {
        return Store.ReplaceStatus.MISS_PRESENT;
      } else {
        return Store.ReplaceStatus.MISS_NOT_PRESENT;
      }
    }
  }

  @Override
  public void clear() throws StoreAccessException {
    delegate.clear();
  }

  @Override
  public StoreEventSource<K, V> getStoreEventSource() {
    return delegate.getStoreEventSource();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() {
    return delegate.iterator();
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws StoreAccessException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public ValueHolder<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public ValueHolder<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) throws StoreAccessException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
    return null;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, Supplier<Boolean> replaceEqual) throws StoreAccessException {
    return null;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
    return null;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return delegate.getConfigurationChangeListeners();
  }

  private V loadFromLoaderWriter(K key, V inCache) {
    if (inCache == null) {
      if (useLoaderInAtomics) {
        try {
          inCache = cacheLoaderWriter.load(key);
          if (inCache == null) {
            return null;
          }
        } catch (Exception e) {
          throw new StorePassThroughException(newCacheLoadingException(e));
        }
      } else {
        return null;
      }
    }
    return inCache;
  }

  private static <K, V> boolean newValueAlreadyExpired(Logger logger, ExpiryPolicy<? super K, ? super V> expiry, K key, V oldValue, V newValue) {
    if (newValue == null) {
      return false;
    }

    Duration duration;
    try {
      if (oldValue == null) {
        duration = expiry.getExpiryForCreation(key, newValue);
      } else {
        duration = expiry.getExpiryForUpdate(key, () -> oldValue, newValue);
      }
    } catch (RuntimeException re) {
      logger.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
      return true;
    }

    return Duration.ZERO.equals(duration);
  }
}
