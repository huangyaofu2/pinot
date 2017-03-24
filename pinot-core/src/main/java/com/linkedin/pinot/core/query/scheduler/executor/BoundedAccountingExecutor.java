/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.query.scheduler.executor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BoundedAccountingExecutor implements ExecutorService {
  private static Logger LOGGER = LoggerFactory.getLogger(BoundedAccountingExecutor.class);
  private final QueryExecutorService delegateExecutor;
  private final Semaphore semaphore;
  private final String tableName;

  BoundedAccountingExecutor(QueryExecutorService s, Semaphore semaphore, String tableName) {
    this.delegateExecutor = s;
    this.semaphore = semaphore;
    this.tableName = tableName;
  }

  @Override
  public void shutdown() {
    delegateExecutor.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegateExecutor.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegateExecutor.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegateExecutor.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException {
    return delegateExecutor.awaitTermination(timeout,unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegateExecutor.submit(new Callable<T>() {
      @Override
      public T call()
          throws Exception {
        long startTime = 
        try {

        } finally {

        }
      }
    });
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegateExecutor.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegateExecutor.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return delegateExecutor.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return delegateExecutor.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return delegateExecutor.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return delegateExecutor.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    delegateExecutor.execute(command);
  }
}
