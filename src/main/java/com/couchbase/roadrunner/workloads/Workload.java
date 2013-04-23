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

package com.couchbase.roadrunner.workloads;

import com.couchbase.client.CouchbaseClient;
import com.google.common.base.Stopwatch;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Workload implements Runnable {

  /** Configure a reusable logger. */
  private final Logger logger =
    LoggerFactory.getLogger(Workload.class.getName());

  /** Reference to the CouchbaseClient */
  private final CouchbaseClient client;

  /** Name of the Workload */
  private final String workloadName;

  /** Counter of total measured ops */
  private long measuredOps;

  /** Counter of total ops */
  private long totalOps;

  /** Ramp time */
  private long ramp;

  /** Total runtime of this workload thread */
  private Stopwatch elapsed;

  /** Document Size */
  private int size;

  /** Measures */
  private Map<String, List<Stopwatch>> measures;

  public Workload(final CouchbaseClient client, final String name,
    final int ramp, final int size) {
    this.client = client;
    this.workloadName = name;
    this.measures = new HashMap<String, List<Stopwatch>>();
    this.measuredOps = 0;
    this.totalOps = 0;
    this.ramp = ramp;
    this.elapsed = new Stopwatch();
    this.size = size;
  }

  public long getTotalOps() {
    return totalOps;
  }

  public void incrTotalOps() {
    totalOps++;
  }

  public void startTimer() {
    elapsed.start();
  }

  public void endTimer() {
    elapsed.stop();
  }

  /**
   * Store a measure for later retrieval.
   *
   * If the ramp-up time is not yet through, don't measure the
   * operation.
   *
   * @param identifier Identifier of the stopwatch.
   * @param watch The stopwatch.
   */
  public void addMeasure(String identifier, Stopwatch watch) {
    if (elapsed.elapsed(TimeUnit.SECONDS) < ramp) {
      return;
    }

    if(!measures.containsKey(identifier)) {
      measures.put(identifier, new ArrayList<Stopwatch>());
    }
    measures.get(identifier).add(watch);
    measuredOps++;
  }

  public Map<String, List<Stopwatch>> getMeasures() {
    return measures;
  }

  public long getMeasuredOps() {
    return measuredOps;
  }

  public Stopwatch totalElapsed() {
    if(elapsed.isRunning()) {
      throw new IllegalStateException("Stopwatch still running!");
    }
    return elapsed;
  }

  /**
   * @return the client
   */
  public CouchbaseClient getClient() {
    return client;
  }

  /**
   * @return the workloadName
   */
  public String getWorkloadName() {
    return workloadName;
  }

  /**
   * @return the logger
   */
  public Logger getLogger() {
    return logger;
  }

  public String randomKey() {
    return UUID.randomUUID().toString();
  }

  protected SampleDocument randomDocument() {
    return new SampleDocument(this.size);
  }

  static class SampleDocument implements Serializable {

    private final byte[] payload;

    public SampleDocument(int payloadSize) {
      byte[] bytes = new byte[payloadSize];
      new Random().nextBytes(bytes);
      this.payload = bytes;
    }

  }


}
