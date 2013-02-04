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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GetSetWorkload extends Workload {

  /** Amount of documents to set/get. */
  private final long amount;

  /** Ratio between get and set calls. */
  private final int ratio;

  /** Benchmark information */
  private List<Stopwatch> getMeasures;
  private List<Stopwatch> setMeasures;


  public GetSetWorkload(CouchbaseClient client, String name, long amount,
    int ratio) {
    super(client, name);
    this.amount = amount;
    this.ratio = ratio;

    this.getMeasures = new ArrayList<Stopwatch>();
    this.setMeasures = new ArrayList<Stopwatch>();
  }

  @Override
  public void run() {
    Thread.currentThread().setName(getWorkloadName());
    CouchbaseClient client = getClient();

    for(long i=0;i<amount;i++) {
      String key = randomKey();
      try {
        Stopwatch setWatch = new Stopwatch().start();
        client.set(key, 0, "hello World").get();
        setWatch.stop();
        setMeasures.add(setWatch);
        for(int r=0;r<ratio;r++) {
          Stopwatch getWatch = new Stopwatch().start();
          client.get(key);
          getWatch.stop();
          getMeasures.add(getWatch);
        }
      } catch (Exception ex) {
        getLogger().info("Problem while set/get key" + ex.getMessage());
      }
    }
  }

  public Map<String, List<Stopwatch>> getMeasures() {
    Map<String, List<Stopwatch>> measures =
      new HashMap<String, List<Stopwatch>>();
    measures.put("get", getMeasures);
    measures.put("set", setMeasures);
    return measures;
  }

  private String randomKey() {
    return UUID.randomUUID().toString();
  }

}
