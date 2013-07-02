/**
 * Copyright (C) 2009-2013 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.roadrunner;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.roadrunner.workloads.Workload;
import com.couchbase.roadrunner.workloads.Workload.DocumentFactory;
import com.google.common.base.Stopwatch;

/**
 * The ClientHandler is responsible for managing its own thread pool and
 * dispatching the given workloads to run.
 */
class ClientHandler {

  /** The global configuration object. */
  private final GlobalConfig config;

  /** The ThreadPoolExecutor for managing threads. */
  private final ThreadPoolExecutor executor;

  /** Its own CouchbaseClient object. */
  private final CouchbaseClient client;

  /** The identifier of this ClientHandler. */
  private final String id;

  /** Number of documents to perform against in this handler. */
  private final long numDocs;

  /** List of deployed workloads. */
  private List<Workload> workloads;

  /** Used to store the merged measures after runs. */
  private Map<String, List<Stopwatch>> mergedMeasures;

  /**
   * Initialize the ClientHandler object.
   *
   * @param config the global configuration object.
   */
  public ClientHandler(final GlobalConfig config, final String id,
    final long numDocs)
    throws Exception {
    this.config = config;
    this.id = id;
    this.numDocs = numDocs;
    this.client = new CouchbaseClient(config.getNodes(), config.getBucket(),
      config.getPassword());
    this.executor = new ThreadPoolExecutor(
      config.getNumThreads(),
      config.getNumThreads(),
      1,
      TimeUnit.HOURS,
      new ArrayBlockingQueue<Runnable>(config.getNumThreads(), true),
      new ThreadPoolExecutor.CallerRunsPolicy()
    );
    this.workloads = new ArrayList<Workload>();
    this.mergedMeasures = new HashMap<String, List<Stopwatch>>();
  }

  /**
   * Execute the given workload against the workers.
   *
   * @param clazz the Workload class name.
   * @throws Exception
   */
  public void executeWorkload(Class<? extends Workload> clazz, DocumentFactory documentFactory) throws Exception {
    long docsPerThread =  (long)Math.floor(numDocs/config.getNumThreads());
    Constructor<? extends Workload> constructor = clazz.getConstructor(
      CouchbaseClient.class, String.class, long.class, int.class, int.class,
      int.class, DocumentFactory.class);
    for(int i=0;i<config.getNumThreads();i++) {
     Workload workload = constructor.newInstance(this.client,
       this.id + "/Workload-" + (i+1), docsPerThread, config.getRatio(),
       config.getSampling(), config.getRamp(), documentFactory);
      workloads.add(workload);
      executor.execute(workload);
    }
  }

  /**
   * Cleanup after workload execution and store the measures.
   *
   * @throws Exception
   */
  public void cleanup() throws Exception {
    while (true) {
      if (executor.getActiveCount() == 0) {
        executor.shutdown();
        break;
      }
    }
    executor.awaitTermination(1, TimeUnit.MINUTES);
    storeMeasures();
    this.client.shutdown();
  }

  /**
   * Aggregate and store the calculated measurements.
   */
  private void storeMeasures() {
    for(Workload workload : workloads) {
      Map<String, List<Stopwatch>> measures = workload.getMeasures();
      for (Map.Entry<String, List<Stopwatch>> entry : measures.entrySet()) {
        if(mergedMeasures.containsKey(entry.getKey())) {
          mergedMeasures.get(entry.getKey()).addAll(entry.getValue());
        } else {
          mergedMeasures.put(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  /**
   * Returns the aggregated measures.
   * @return the measures.
   */
  public Map<String, List<Stopwatch>> getMeasures() {
    return mergedMeasures;
  }

  public long getTotalOps() {
    long totalOps = 0;
    for(Workload workload : workloads) {
      totalOps += workload.getTotalOps();
    }
    return totalOps;
  }

  public long getMeasuredOps() {
    long measuredOps = 0;
    for(Workload workload : workloads) {
      measuredOps += workload.getMeasuredOps();
    }
    return measuredOps;
  }

  public List<Stopwatch> getThreadElapsed() {
    List<Stopwatch> elapsed = new ArrayList<Stopwatch>();
    for(Workload workload : workloads) {
      elapsed.add(workload.totalElapsed());
    }
    return elapsed;
  }

}
