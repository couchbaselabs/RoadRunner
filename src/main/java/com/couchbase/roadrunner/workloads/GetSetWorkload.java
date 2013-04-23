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

public class GetSetWorkload extends Workload {

  /** Amount of documents to set/get. */
  private final long amount;

  /** Ratio between get and set calls. */
  private final int ratio;

  /** Ratio to sample statistics data. */
  private final int sampling;


  public GetSetWorkload(CouchbaseClient client, String name, long amount,
    int ratio, int sampling, int ramp) {
    super(client, name, ramp);
    this.amount = amount;
    this.ratio = ratio;
    this.sampling = 100/sampling;
  }

  @Override
  public void run() {
    Thread.currentThread().setName(getWorkloadName());
    startTimer();

    int samplingCount = 0;
    for(long i=0;i<amount;i++) {
      String key = randomKey();
      try {
        if(++samplingCount == sampling) {
          setWorkloadWithMeasurement(key);
          for(int r=0;r<ratio;r++) {
            getWorkloadWithMeasurement(key);
          }
          samplingCount = 0;
        } else {
          setWorkload(key);
          for(int r=0;r<ratio;r++) {
            getWorkload(key);
          }
        }
      } catch (Exception ex) {
        getLogger().info("Problem while set/get key" + ex.getMessage());
      }
    }

    endTimer();
  }

  private void setWorkloadWithMeasurement(String key) throws Exception {
    Stopwatch watch = new Stopwatch().start();
    setWorkload(key);
    watch.stop();
    addMeasure("set", watch);
  }

  private void setWorkload(String key) throws Exception {
    getClient().set(key, 0, new SampleDocument(10000)).get();
    incrTotalOps();
  }

  private void getWorkloadWithMeasurement(String key) throws Exception {
    Stopwatch watch = new Stopwatch().start();
    getWorkload(key);
    watch.stop();
    addMeasure("get", watch);
  }

  private void getWorkload(String key) throws Exception {
    getClient().get(key);
    incrTotalOps();
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
