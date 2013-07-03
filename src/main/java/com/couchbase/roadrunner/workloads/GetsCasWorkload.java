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
import com.couchbase.roadrunner.workloads.Workload.DocumentFactory;
import com.google.common.base.Stopwatch;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;

import java.io.Serializable;
import java.util.*;

/**
 * The GetsCasWorkload resembles a use case where a document is added, and then
 * consequently loaded, updated and stored with CAS values.
 *
 * It uses the "add" command for storing, and then fetching the doc with
 * "gets" and saving it back with "cas".
 */
public class GetsCasWorkload extends Workload {

  /** Amount of documents to add/gets/cas. */
  private final long amount;

  /** Ratio between add and gets/cas calls. */
  private final int ratio;

  /** Ratio to sample statistics data. */
  private final int sampling;

  public GetsCasWorkload(CouchbaseClient client, String name, long amount,
    int ratio, int sampling, int ramp, DocumentFactory documentFactory) {
    super(client, name, ramp, documentFactory);
    this.amount = amount;
    this.ratio = ratio;
    this.sampling = 100 / sampling;
  }

  @Override
  public void run() {
    Thread.currentThread().setName(getWorkloadName());
    CouchbaseClient client = getClient();
    startTimer();

    int samplingCount = 0;
    for (long i=0;i < amount;i++) {
      String key = randomKey();
      try {
        addWorkload(key, getDocument());
        if(++samplingCount == sampling) {
          for(int r=0;r<ratio;r++) {
            long cas = getsWorkloadWithMeasurement(key);
            casWorkloadWithMeasurement(key, cas, getDocument());
          }
          samplingCount = 0;
        } else {
          for(int r=0;r<ratio;r++) {
            long cas = getsWorkload(key);
            casWorkload(key, cas,  getDocument());
          }
        }
      } catch (Exception ex) {
        getLogger().info("Problem while gets/cas key: " + ex.getMessage());
      }
    }

    endTimer();
  }

  private String randomString() {
    int length = 10000;
    StringBuffer outputBuffer = new StringBuffer(length);
    for (int i = 0; i < length; i++){
      outputBuffer.append(" ");
    }
    return outputBuffer.toString();
  }

  private void addWorkload(String key, SampleDocument doc) throws Exception {
    CouchbaseClient client = getClient();
    client.add(key, 0, doc).get();
    incrTotalOps();
  }

  private long getsWorkloadWithMeasurement(String key) {
    Stopwatch watch = new Stopwatch().start();
    long cas = getsWorkload(key);
    watch.stop();
    addMeasure("gets", watch);
    return cas;
  }

  private void casWorkloadWithMeasurement(String key, long cas, SampleDocument doc) {
    Stopwatch watch = new Stopwatch().start();
    casWorkload(key, cas, doc);
    watch.stop();
    addMeasure("cas", watch);
  }

  private long getsWorkload(String key) {
    CouchbaseClient client = getClient();
    CASValue<Object> casResponse = client.gets(key);
    incrTotalOps();
    return casResponse.getCas();
  }

  private void casWorkload(String key, long cas, SampleDocument doc) {
    CouchbaseClient client = getClient();
    CASResponse response = client.cas(key, cas, doc);
    if(response != CASResponse.OK) {
      getLogger().info("Could not store with cas for key: " + key);
    }
    incrTotalOps();
  }

}
