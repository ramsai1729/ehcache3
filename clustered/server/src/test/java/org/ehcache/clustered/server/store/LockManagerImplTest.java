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
package org.ehcache.clustered.server.store;

import org.ehcache.clustered.server.TestClientDescriptor;
import org.junit.Test;
import org.terracotta.entity.ClientDescriptor;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class LockManagerImplTest {

  @Test
  public void testLock() {
    LockManagerImpl lockManager = new LockManagerImpl();
    ClientDescriptor clientDescriptor = new TestClientDescriptor();
    assertThat(lockManager.lock(1L, clientDescriptor), is(true));
    assertThat(lockManager.lock(1L, clientDescriptor), is(false));
    assertThat(lockManager.lock(2L, clientDescriptor), is(true));
  }

  @Test
  public void testUnlock() {
    LockManagerImpl lockManager = new LockManagerImpl();
    ClientDescriptor clientDescriptor = new TestClientDescriptor();
    assertThat(lockManager.lock(1L, clientDescriptor), is(true));
    lockManager.unlock(1L);
    assertThat(lockManager.lock(1L, clientDescriptor), is(true));
  }

  @Test
  public void testSweepLocksForClient() {
    LockManagerImpl lockManager = new LockManagerImpl();
    ClientDescriptor clientDescriptor1 = new TestClientDescriptor();
    ClientDescriptor clientDescriptor2 = new TestClientDescriptor();

    assertThat(lockManager.lock(1L, clientDescriptor1), is(true));
    assertThat(lockManager.lock(2L, clientDescriptor1), is(true));
    assertThat(lockManager.lock(3L, clientDescriptor1), is(true));
    assertThat(lockManager.lock(4L, clientDescriptor1), is(true));
    assertThat(lockManager.lock(5L, clientDescriptor2), is(true));
    assertThat(lockManager.lock(6L, clientDescriptor2), is(true));

    lockManager.sweepLocksForClient(clientDescriptor2);

    assertThat(lockManager.lock(5L, clientDescriptor2), is(true));
    assertThat(lockManager.lock(6L, clientDescriptor2), is(true));
    assertThat(lockManager.lock(1L, clientDescriptor1), is(false));
    assertThat(lockManager.lock(2L, clientDescriptor1), is(false));
    assertThat(lockManager.lock(3L, clientDescriptor1), is(false));
    assertThat(lockManager.lock(4L, clientDescriptor1), is(false));

  }

}