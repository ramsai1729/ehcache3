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

import java.util.Map;

interface BatchOperation<K, V> {

  void execute(CacheLoaderWriter<? super K, V> cacheLoaderWriter) throws Exception;


  class WriteAllOperation<K, V> implements BatchOperation<K, V> {

    private final Iterable<? extends Map.Entry<? extends K, ? extends V>> writableEntries;

    WriteAllOperation(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {this.writableEntries = entries;}

    @Override
    public void execute(CacheLoaderWriter<? super K, V> cacheLoaderWriter) throws Exception {
      cacheLoaderWriter.writeAll(writableEntries);
    }
  }

  class DeleteAllOperation<K, V> implements BatchOperation<K, V> {

    private final Iterable<? extends K> deletedKeys;

    DeleteAllOperation(Iterable<? extends K> deletedKeys) {this.deletedKeys = deletedKeys;}

    @Override
    public void execute(CacheLoaderWriter<? super K, V> cacheLoaderWriter) throws Exception {
      cacheLoaderWriter.deleteAll(deletedKeys);
    }
  }
}
