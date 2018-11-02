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

import org.ehcache.clustered.client.internal.loaderwriter.writebehind.BatchOperation.DeleteAllOperation;
import org.ehcache.clustered.client.internal.loaderwriter.writebehind.BatchOperation.WriteAllOperation;
import org.ehcache.clustered.client.internal.loaderwriter.writebehind.SingleOperation.WriteOperation;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration.BatchingConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BatchedOperationProcessor<K, V> implements WriteBehindOperationProcessor<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedOperationProcessor.class);

  private final int batchSize;
  private final boolean coalescing;
  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;
  private volatile Batch<K, V> currentBatch;

  BatchedOperationProcessor(BatchingConfiguration batchingConfiguration,
                            CacheLoaderWriter<? super K, V> cacheLoaderWriter) {
    this.batchSize = batchingConfiguration.getBatchSize();
    this.coalescing = batchingConfiguration.isCoalescing();
    this.cacheLoaderWriter = cacheLoaderWriter;
  }

  @Override
  public void add(SingleOperation<K, V> operation) {
    if (currentBatch == null) {
      currentBatch = newBatch();
    }

    if (currentBatch.add(operation)) {
      currentBatch.execute();
      currentBatch = null;
    }
  }

  private Batch<K, V> newBatch() {
    return coalescing ? new CoalescingBatch<>(batchSize, cacheLoaderWriter) : new SimpleBatch<>(batchSize, cacheLoaderWriter);
  }

  private static abstract class Batch<K, V> {

    private final int size;
    private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;

    Batch(int size, CacheLoaderWriter<? super K, V> cacheLoaderWriter) {
      this.size = size;
      this.cacheLoaderWriter = cacheLoaderWriter;
    }

    boolean add(SingleOperation<K, V> operation) {
      internalAdd(operation);
      return getSize() >= size;
    }

    void execute() {
      Iterable<SingleOperation<K, V>> operations = getOperations();
      List<BatchOperation<K, V>> batches = createBatches(operations);
      for (BatchOperation<K, V> batch : batches) {
        try {
          batch.execute(cacheLoaderWriter);
        } catch (Exception e) {
          LOGGER.warn("exception while updating the SOR", e);
        }
      }
    }

    private List<BatchOperation<K, V>> createBatches(Iterable<SingleOperation<K, V>> operations) {
      final List<BatchOperation<K, V>> closedBatches = new ArrayList<>();

      Set<K> activeDeleteKeys = new HashSet<>();
      Set<K> activeWrittenKeys = new HashSet<>();
      List<K> activeDeleteBatch = new ArrayList<>();
      List<Map.Entry<K, V>> activeWriteBatch = new ArrayList<>();

      for (SingleOperation<K, V> item : operations) {
        if (item instanceof WriteOperation) {
          if (activeDeleteKeys.contains(item.getKey())) {
            //close the current delete batch
            closedBatches.add(new DeleteAllOperation<>(activeDeleteBatch));
            activeDeleteBatch = new ArrayList<>();
            activeDeleteKeys = new HashSet<>();
          }
          activeWriteBatch.add(new AbstractMap.SimpleEntry<>(item.getKey(), ((WriteOperation<K, V>)item).getValue()));
          activeWrittenKeys.add(item.getKey());
        } else if (item instanceof org.ehcache.impl.internal.loaderwriter.writebehind.operations.DeleteOperation) {
          if (activeWrittenKeys.contains(item.getKey())) {
            //close the current write batch
            closedBatches.add(new WriteAllOperation<>(activeWriteBatch));
            activeWriteBatch = new ArrayList<>();
            activeWrittenKeys = new HashSet<>();
          }
          activeDeleteBatch.add(item.getKey());
          activeDeleteKeys.add(item.getKey());
        } else {
          throw new AssertionError();
        }
      }

      if (!activeWriteBatch.isEmpty()) {
        closedBatches.add(new WriteAllOperation<>(activeWriteBatch));
      }
      if (!activeDeleteBatch.isEmpty()) {
        closedBatches.add(new DeleteAllOperation<>(activeDeleteBatch));
      }
      return closedBatches;
    }

    abstract void internalAdd(SingleOperation<K, V> operation);

    abstract Iterable<SingleOperation<K, V>> getOperations();

    abstract int getSize();
  }

  private static class SimpleBatch<K, V> extends Batch<K, V> {

    List<SingleOperation<K, V>> operations = new ArrayList<>();

    SimpleBatch(int size, CacheLoaderWriter<? super K, V> cacheLoaderWriter) {
      super(size, cacheLoaderWriter);
    }

    @Override
    void internalAdd(SingleOperation<K, V> operation) {
      operations.add(operation);
    }

    @Override
    Iterable<SingleOperation<K, V>> getOperations() {
      return operations;
    }

    @Override
    int getSize() {
      return operations.size();
    }
  }

  private static class CoalescingBatch<K, V> extends Batch<K, V> {

    LinkedHashMap<K, SingleOperation<K, V>> operationMap = new LinkedHashMap<>();

    CoalescingBatch(int size, CacheLoaderWriter<? super K, V> cacheLoaderWriter) {
      super(size, cacheLoaderWriter);
    }

    @Override
    void internalAdd(SingleOperation<K, V> operation) {
      operationMap.put(operation.getKey(), operation);
    }

    @Override
    Iterable<SingleOperation<K, V>> getOperations() {
      return operationMap.values();
    }

    @Override
    int getSize() {
      return operationMap.size();
    }
  }
}
