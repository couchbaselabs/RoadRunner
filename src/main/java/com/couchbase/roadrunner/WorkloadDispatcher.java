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

import com.couchbase.roadrunner.workloads.GetSetWorkload;
import com.couchbase.roadrunner.workloads.GetsCasWorkload;
import com.couchbase.roadrunner.workloads.Workload;
import com.couchbase.roadrunner.workloads.WorkloadFactory;
import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The WorkloadDispatcher is responsible for initializing the Clients, their
 * corresponding workers and initializing the workload.
 */
final class WorkloadDispatcher {

  /** Configure a reusable logger. */
  static final Logger LOGGER =
    LoggerFactory.getLogger(WorkloadDispatcher.class.getName());

  /** The global configuration object. */
  private final GlobalConfig config;

  /** Links to the clientHandlers for each CouchabaseClient. */
  private List<ClientHandler> clientHandlers;

  Map<String, List<Stopwatch>> mergedMeasures;

  /**
   * Create the WorkloadDispatcher object.
   *
   * @param config The global configuration object with all settings.
   */
  public WorkloadDispatcher(final GlobalConfig config) {
    this.config = config;
    this.clientHandlers = new ArrayList<ClientHandler>();
    this.mergedMeasures = new HashMap<String, List<Stopwatch>>();
  }

  /**
   * Initialize and run the ClientHandlers.
   */
  public void init() throws Exception {
    long docsPerHandler = (long)Math.floor(
      config.getNumDocs()/config.getNumClients());
    for(int i=0;i<config.getNumClients();i++) {
      clientHandlers.add(new ClientHandler(config, "ClientHandler-"+(i+1),
        docsPerHandler));
    }
  }

  /**
   * Distribute and run the workload against the ClientHandlers.
   */
  public void dispatchWorkload() throws Exception {
    Class<? extends Workload> clazz =
      WorkloadFactory.getWorkload(config.getWorkload());
    for(ClientHandler handler : clientHandlers) {
      handler.executeWorkload(clazz);
    }
    for(ClientHandler handler : clientHandlers) {
      handler.cleanup();
    }
  }

  public void prepareMeasures() {
    storeMeasures();
  }

  private void storeMeasures() {
    for(ClientHandler handler : clientHandlers) {
      Map<String, List<Stopwatch>> measures = handler.getMeasures();
      for (Map.Entry<String, List<Stopwatch>> entry : measures.entrySet()) {
        List<Stopwatch> stored;
        if(mergedMeasures.containsKey(entry.getKey())) {
          mergedMeasures.get(entry.getKey()).addAll(entry.getValue());
        } else {
          mergedMeasures.put(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  public Map<String, List<Stopwatch>> getMeasures() {
    return mergedMeasures;
  }

  public long getTotalOps() {
    long totalOps = 0;
    for (ClientHandler handler : clientHandlers) {
      totalOps += handler.getTotalOps();
    }
    return totalOps;
  }
}
