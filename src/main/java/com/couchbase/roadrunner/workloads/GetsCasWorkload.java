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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.error.CASMismatchException;
import com.google.common.base.Stopwatch;

import rx.Observable;

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
    CountDownLatch latch = new CountDownLatch(1);

    int samplingCount = 0;
    for (long i=0;i < amount;i++) {
      String key = randomKey();
      boolean last = i == amount-1;

      if(++samplingCount == sampling) {
        addWorkload(key, getDocument())
            .flatMap(d -> getsWorkloadWithMeasurement(key).repeat(ratio))
            .flatMap(cas -> casWorkloadWithMeasurement(key, cas, getDocument()))
            .doOnError(ex -> getLogger().info("Problem while measured gets/cas key: " + ex.getMessage()))
            .finallyDo(() -> { if (last) latch.countDown(); })
        .subscribe();
        samplingCount = 0;
      } else {
        addWorkload(key, getDocument())
            .flatMap(d -> getsWorkload(key).repeat(ratio))
            .flatMap(cas -> casWorkload(key, cas, getDocument()))
            .doOnError(ex -> getLogger().info("Problem while gets/cas key: " + ex.getMessage()))
            .finallyDo(() -> { if (last) latch.countDown(); })
        .subscribe();
      }
    }

    try {
      latch.await(5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      endTimer();
    }
  }

  private String randomString() {
    int length = 10000;
    StringBuffer outputBuffer = new StringBuffer(length);
    for (int i = 0; i < length; i++){
      outputBuffer.append(" ");
    }
    return outputBuffer.toString();
  }

  private Observable<LegacyDocument> addWorkload(String key, SampleDocument payload) {
    LegacyDocument doc = LegacyDocument.create(key, 0, payload);
    return Observable.defer(() ->
            getBucket()
                .upsert(doc)
                .doOnNext(item -> incrTotalOps())
    );
  }

  private Observable<Long> getsWorkloadWithMeasurement(String key) {
    return Observable.defer(() -> {
      Stopwatch watch = new Stopwatch().start();
      return getsWorkload(key)
          .finallyDo(() -> {
            watch.stop();
            addMeasure("gets", watch);
          });
    });
  }

  private Observable<LegacyDocument> casWorkloadWithMeasurement(String key, long cas, SampleDocument doc) {
    return Observable.defer(() -> {
      Stopwatch watch = new Stopwatch().start();
      return casWorkload(key, cas, doc)
          .finallyDo(() -> {
            watch.stop();
            addMeasure("cas", watch);
          });
    });
  }

  private Observable<Long> getsWorkload(String key) {
    return Observable.defer(() ->
      getBucket()
        .get(key, LegacyDocument.class)
        .map(doc -> doc.cas())
        .doOnNext(item -> incrTotalOps())
    );
  }

  private Observable<LegacyDocument> casWorkload(String key, long cas, SampleDocument payload) {
    LegacyDocument doc = LegacyDocument.create(key, payload, cas);
    return Observable.defer(() ->
      getBucket()
        .replace(doc)
        .doOnNext(item -> incrTotalOps())
          .doOnError(ex -> {
            if (ex instanceof CASMismatchException)
              getLogger().info("Could not store with cas for key: " + key);
            else
              getLogger().info("Unexpected error while storing cas for key " + key + " : " + ex);
          })
        .onErrorReturn(ex -> doc)
    );
  }

}