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

  /** Number of total operations executed. */
  private long totalOps;

  /** Benchmark information */
  private List<Stopwatch> casMeasures;
  private List<Stopwatch> getsMeasures;

  public GetsCasWorkload(CouchbaseClient client, String name, long amount,
    int ratio, int sampling) {
    super(client, name);
    this.amount = amount;
    this.ratio = ratio;
    this.sampling = 100 / sampling;

    this.casMeasures = new ArrayList<Stopwatch>();
    this.getsMeasures = new ArrayList<Stopwatch>();
    this.totalOps = 0;
  }

  @Override
  public void run() {
    Thread.currentThread().setName(getWorkloadName());
    CouchbaseClient client = getClient();

    int samplingCount = 0;
    for (long i=0;i < amount;i++) {
      String key = randomKey();
      try {
        addWorkload(key, randomDocument());
        if(++samplingCount == sampling) {
          for(int r=0;r<ratio;r++) {
            long cas = getsWorkloadWithMeasurement(key);
            casWorkloadWithMeasurement(key, cas, randomDocument());
          }
          samplingCount = 0;
        } else {
          for(int r=0;r<ratio;r++) {
            long cas = getsWorkload(key);
            casWorkload(key, cas,  randomDocument());
          }
        }
      } catch (Exception ex) {
        getLogger().info("Problem while set/get key: " + ex.getMessage());
      }
    }
  }

  private String randomKey() {
    return UUID.randomUUID().toString();
  }

  private String randomString() {
    int length = 10000;
    StringBuffer outputBuffer = new StringBuffer(length);
    for (int i = 0; i < length; i++){
      outputBuffer.append(" ");
    }
    return outputBuffer.toString();
  }

  private SampleDocument randomDocument() {
    return new SampleDocument(10000);
  }

  private void addWorkload(String key, SampleDocument doc) throws Exception {
    CouchbaseClient client = getClient();
    client.add(key, 0, doc).get();
    totalOps++;
  }

  private long getsWorkloadWithMeasurement(String key) {
    Stopwatch watch = new Stopwatch().start();
    long cas = getsWorkload(key);
    watch.stop();
    getsMeasures.add(watch);
    return cas;
  }

  private void casWorkloadWithMeasurement(String key, long cas, SampleDocument doc) {
    Stopwatch watch = new Stopwatch().start();
    casWorkload(key, cas, doc);
    watch.stop();
    casMeasures.add(watch);
  }

  private long getsWorkload(String key) {
    CouchbaseClient client = getClient();
    CASValue<Object> casResponse = client.gets(key);
    totalOps++;
    return casResponse.getCas();
  }

  private void casWorkload(String key, long cas, SampleDocument doc) {
    CouchbaseClient client = getClient();
    CASResponse response = client.cas(key, cas, doc);
    if(response != CASResponse.OK) {
      getLogger().info("Could not store with cas for key: " + key);
    }
    totalOps++;
  }

  public final Map<String, List<Stopwatch>> getMeasures() {
    Map<String, List<Stopwatch>> measures = new HashMap<String, List<Stopwatch>>();
    measures.put("cas", casMeasures);
    measures.put("gets", getsMeasures);
    return measures;
  }

  public long getTotalOps() {
    return this.totalOps;
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
