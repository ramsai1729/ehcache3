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
package org.ehcache.clustered.client.internal.loaderwriter.writebehind;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

interface SingleOperation<K, V> {
  K getKey();

  void execute(CacheLoaderWriter<? super K, V> cacheLoaderWriter) throws Exception;

  class WriteOperation<K, V> implements SingleOperation<K, V> {

    private final K key;
    private final V value;

    WriteOperation(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    @Override
    public void execute(CacheLoaderWriter<? super K, V> cacheLoaderWriter) throws Exception {
      cacheLoaderWriter.write(key, value);
    }
  }

  class DeleteOperation<K, V> implements SingleOperation<K, V> {

    private final K key;

    DeleteOperation(K key) {
      this.key = key;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public void execute(CacheLoaderWriter<? super K, V> cacheLoaderWriter) throws Exception {
      cacheLoaderWriter.delete(key);
    }
  }
}
