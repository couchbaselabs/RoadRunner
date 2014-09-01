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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.error.CASMismatchException;
import com.google.common.base.Stopwatch;

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

  public GetsCasWorkload(Bucket bucket, String name, long amount,
    int ratio, int sampling, int ramp, DocumentFactory documentFactory) {
    super(bucket, name, ramp, documentFactory);
    this.amount = amount;
    this.ratio = ratio;
    this.sampling = 100 / sampling;
  }

  @Override
  public void run() {
    Thread.currentThread().setName(getWorkloadName());
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

  private void addWorkload(String key, SampleDocument payload) throws Exception {
    LegacyDocument doc = LegacyDocument.create(key, 0, payload);
    getBucket().upsert(doc).toBlocking().single();
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
    LegacyDocument doc = getBucket().get(key, LegacyDocument.class).toBlocking().single();
    incrTotalOps();
    return doc.cas();
  }

  private void casWorkload(String key, long cas, SampleDocument payload) {
    LegacyDocument doc = LegacyDocument.create(key, payload, cas);
    try {
      getBucket().replace(doc).toBlocking().single();
    } catch (CASMismatchException e) {
      getLogger().info("Could not store with cas for key: " + key);
    }
    incrTotalOps();
  }

}
