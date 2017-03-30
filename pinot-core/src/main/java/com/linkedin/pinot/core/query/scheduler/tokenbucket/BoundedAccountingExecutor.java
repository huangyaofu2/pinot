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

package com.linkedin.pinot.core.query.scheduler.tokenbucket;

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
  private final ExecutorService delegateExecutor;
  private Semaphore semaphore;
  private final String tableName;

  public BoundedAccountingExecutor(ExecutorService s, Semaphore semaphore, String tableName) {
    this.delegateExecutor = s;
    this.semaphore = semaphore;
    this.tableName = tableName;
  }

  public BoundedAccountingExecutor(ExecutorService s, String tableName) {
    this.delegateExecutor = s;
    this.tableName = tableName;
  }

  public void setBounds(Semaphore s) {
    this.semaphore = s;
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
    return delegateExecutor.submit(toAccountingCallable(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegateExecutor.submit(toAccoutingRunnable(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegateExecutor.submit(toAccoutingRunnable(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    throw new UnsupportedOperationException("invoke all on bounded executor is not supported");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    throw new UnsupportedOperationException("invoke all on bounded executor is not supported");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException("invoke all on bounded executor is not supported");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    throw new UnsupportedOperationException("invoke all on bounded executor is not supported");
  }

  @Override
  public void execute(Runnable command) {
    delegateExecutor.execute(toAccoutingRunnable(command));
  }

  private <T> QueryAccountingCallable<T> toAccountingCallable(Callable<T> callable) {
    acquirePermits(1);
    return new QueryAccountingCallable<>(callable, semaphore);
  }

  private QueryAccountingRunnable toAccoutingRunnable(Runnable runnable) {
    acquirePermits(1);
    return new QueryAccountingRunnable(runnable, semaphore);
  }

  private void acquirePermits(int permits) {
    try {
      semaphore.acquire(permits);
    } catch (InterruptedException e) {
      LOGGER.error("Thread interrupted while waiting for semaphore", e);
      throw new RuntimeException(e);
    }
  }
}

class QueryAccountingRunnable implements Runnable {
  private final Runnable runnable;
  private final Semaphore semaphore;

  QueryAccountingRunnable(Runnable r, Semaphore semaphore) {
    this.runnable = r;
    this.semaphore = semaphore;
  }

  @Override
  public void run() {
    long startTime = System.nanoTime();
    try {
      runnable.run();
    } finally {
      long totalTime = System.nanoTime() - startTime;
      semaphore.release();
    }
  }
}

class QueryAccountingCallable<T> implements Callable<T> {

  private final Callable<T> callable;
  private final Semaphore semaphore;

  QueryAccountingCallable(Callable<T> c, Semaphore semaphore) {
    this.callable = c;
    this.semaphore = semaphore;
  }
  @Override
  public T call()
      throws Exception {
    long startTime = System.nanoTime();
    try {
      return callable.call();
    } finally {
      long totalTime = System.nanoTime();
      semaphore.release();
    }
  }
}
