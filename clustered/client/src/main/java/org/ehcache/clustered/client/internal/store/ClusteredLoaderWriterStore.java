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
package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.client.internal.store.operations.EternalChainResolver;
import org.ehcache.clustered.client.internal.store.operations.PutOperation;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

import static org.ehcache.core.exceptions.ExceptionFactory.newCacheLoadingException;
import static org.ehcache.core.exceptions.ExceptionFactory.newCacheWritingException;
import static org.ehcache.core.exceptions.StorePassThroughException.handleException;

public class ClusteredLoaderWriterStore<K, V> extends ClusteredStore<K, V> implements AuthoritativeTier<K, V> {

  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;

  ClusteredLoaderWriterStore(Configuration<K, V> config, OperationsCodec<K, V> codec, EternalChainResolver<K, V> resolver, TimeSource timeSource,
                             CacheLoaderWriter<? super K, V> loaderWriter) {
    super(config, codec, resolver, timeSource);
    this.cacheLoaderWriter = loaderWriter;
  }

  @Override
  protected ValueHolder<V> getInternal(K key) throws StoreAccessException, TimeoutException {
    ValueHolder<V> holder = super.getInternal(key);
    try {
      if (holder == null) {
        long hash = extractLongKey(key);
        boolean unlocked = false;
        storeProxy.lock(hash);
        try {
          V value = null;
          try {
            value = cacheLoaderWriter.load(key);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          if (value == null) {
            return null;
          }
          appendAndUnlock(key, value);
          unlocked = true;
          return new ClusteredValueHolder<>(value);
        } finally {
          if (!unlocked) {
            storeProxy.unlock(hash);
          }
        }
      }
    } catch (RuntimeException re) {
      throw handleException(re);
    }
    return holder;
  }

  private void appendAndUnlock(K key, V value) throws TimeoutException {
    PutOperation<K, V> operation = new PutOperation<>(key, value, timeSource.getTimeMillis());
    ByteBuffer payload = codec.encode(operation);
    long extractedKey = extractLongKey(key);
    storeProxy.appendAndUnlock(extractedKey, payload);
  }

  @Override
  protected PutStatus silentPut(K key, V value) throws StoreAccessException {
    try {
      long hash = extractLongKey(key);
      boolean unlocked = false;
      storeProxy.lock(hash);
      try {
        cacheLoaderWriter.write(key, value);
        appendAndUnlock(key, value);
        unlocked = true;
      } finally {
        if (!unlocked) {
          storeProxy.unlock(hash);
        }
      }
      return PutStatus.PUT;
    } catch (Exception e) {
      throw handleException(e);
    }
  }

  @Override
  protected boolean silentRemove(K key) throws StoreAccessException {
    return super.silentRemove(key);
  }
}
