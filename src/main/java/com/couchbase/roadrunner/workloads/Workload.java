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
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Workload implements Runnable {

  /** Configure a reusable logger. */
  private final Logger LOGGER =
    LoggerFactory.getLogger(Workload.class.getName());
  private final CouchbaseClient client;
  private final String workloadName;

  public Workload(final CouchbaseClient client, final String name) {
    this.client = client;
    this.workloadName = name;
  }

  abstract public Map<String, List<Stopwatch>> getMeasures();
  abstract public long getTotalOps();

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
    return LOGGER;
  }


}
