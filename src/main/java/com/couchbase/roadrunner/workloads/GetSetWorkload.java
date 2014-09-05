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
import com.google.common.base.Stopwatch;

import rx.Observable;

public class GetSetWorkload extends Workload {

  /** Amount of documents to set/get. */
  private final long amount;

  /** Ratio between get and set calls. */
  private final int ratio;

  /** Ratio to sample statistics data. */
  private final int sampling;


  public GetSetWorkload(Bucket bucket, String name, long amount,
    int ratio, int sampling, int ramp, DocumentFactory documentFactory) {
    super(bucket, name, ramp, documentFactory);
    this.amount = amount;
    this.ratio = ratio;
    this.sampling = 100/sampling;
  }

  @Override
  public void run() {
    Thread.currentThread().setName(getWorkloadName());
    startTimer();
    CountDownLatch latch = new CountDownLatch(1);

    int samplingCount = 0;
    for(long i=0;i<amount;i++) {
      boolean last = i == amount-1;
      String key = randomKey();

        if(++samplingCount == sampling) {
          //launch a measured "set" operation followed by ratio "get" operations, also measured
          setWorkloadWithMeasurement(key)
              .repeat(ratio)
              .flatMap(docInDb -> getWorkloadWithMeasurement(key))
              .doOnError(ex -> getLogger().info("Problem while measured set/get key" + ex.getMessage()))
              //schedule the ending of the timer at the last iteration
              .finallyDo(() -> {
                if (last) latch.countDown();
              })
              .subscribe();

          samplingCount = 0;
        } else {
          //launch a simple "set" operation, followed by ratio "get" operations
          setWorkload(key)
              .repeat(ratio)
              .flatMap(docInDb -> getWorkload(key))
              .doOnError(ex -> getLogger().info("Problem while set/get key" + ex.getMessage()))
              //schedule the ending of the timer at the last iteration
              .finallyDo(() -> {
                if (last) latch.countDown();
              })
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

  private Observable<LegacyDocument> setWorkloadWithMeasurement(String key) {
    return Observable.defer(() -> {
      Stopwatch watch = new Stopwatch().start();
      return  setWorkload(key).doOnTerminate(() -> {
        watch.stop();
        addMeasure("set", watch);
      });
    });
  }

  private Observable<LegacyDocument> setWorkload(String key)  {
    LegacyDocument value = LegacyDocument.create(key, 0, getDocument());
    Observable<LegacyDocument> result = Observable.defer(() ->
        getBucket()
            .insert(value)
            .doOnEach(doc -> incrTotalOps())
    );
    return result;
  }

  private Observable<LegacyDocument> getWorkloadWithMeasurement(String key) {
    return Observable.defer(() -> {
      Stopwatch watch = new Stopwatch().start();
      return getWorkload(key)
            .doOnTerminate(() -> {
              watch.stop();
              addMeasure("get", watch);
            });
    });
  }

  private Observable<LegacyDocument> getWorkload(String key) {
    return Observable.defer(() ->
            getBucket()
                .get(key, LegacyDocument.class)
                .doOnEach(doc -> incrTotalOps())
    );
  }

}
