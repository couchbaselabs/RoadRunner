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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container class for the user-provided options through the command line.
 *
 * It also sets sensible defaults when no or a subset of arguments are
 * provided.
 */
final  class GlobalConfig {

  /** Configure a reusable logger. */
  static final Logger LOGGER =
    LoggerFactory.getLogger(GlobalConfig.class.getName());

  public static final String DEFAULT_NODES = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "default";
  public static final String DEFAULT_PASSWORD = "";
  public static final String DEFAULT_NUM_THREADS = "1";
  public static final String DEFAULT_NUM_CLIENTS = "1";
  public static final String DEFAULT_NUM_DOCS = "1000";
  public static final String DEFAULT_RATIO = "50";
  public static final String DEFAULT_SAMPLING = "100";
  public static final String DEFAULT_WORKLOAD = "getset";
  public static final String DEFAULT_RAMP = "0";
  public static final String DEFAULT_SIZE = "1000";

  private final List<String> nodes;
  private final String bucket;
  private final String password;
  private final int numThreads;
  private final int numClients;
  private final long numDocs;
  private final int ratio;
  private final int sampling;
  private final String workload;
  private final int ramp;
  private final int size;
  private final String filename;

  /**
   * Create the GlobalConfig.
   *
   * @param nodes The list of nodes, as String urls.
   * @param bucket The name of the bucket.
   * @param password The password of the bucket.
   * @param numThreads The number of threads.
   * @param numClients The number of CouchbaseClients.
   */
  private GlobalConfig(List<String> nodes, String bucket, String password,
    int numThreads, int numClients, long numDocs, int ratio, int sampling,
    String workload, int ramp, int size, String filename) {
    this.nodes = Collections.unmodifiableList(nodes);
    this.bucket = bucket;
    this.password = password;
    this.numThreads = numThreads;
    this.numClients = numClients;
    this.numDocs = numDocs;
    this.ratio = ratio;
    this.sampling = sampling;
    this.workload = workload;
    this.ramp = ramp;
    this.size = size;
    this.filename = filename;
  }

  /**
   * Create the GlobalConfig object by parsing the command line args and
   * applying defaults.
   *
   * @param args The passed in command line arguments.
   * @return A immutable GlobalConfig object.
   */
  public static GlobalConfig fromCommandLine(final CommandLine args) {
    String nodes = args.hasOption(RoadRunner.OPT_NODES)
      ? args.getOptionValue(RoadRunner.OPT_NODES) : DEFAULT_NODES;
    String bucket = args.hasOption(RoadRunner.OPT_BUCKET)
      ? args.getOptionValue(RoadRunner.OPT_BUCKET) : DEFAULT_BUCKET;
    String password = args.hasOption(RoadRunner.OPT_PASSWORD)
      ? args.getOptionValue(RoadRunner.OPT_PASSWORD) : DEFAULT_PASSWORD;
    String numThreads = args.hasOption(RoadRunner.OPT_NUM_THREADS)
      ? args.getOptionValue(RoadRunner.OPT_NUM_THREADS) : DEFAULT_NUM_THREADS;
    String numClients = args.hasOption(RoadRunner.OPT_NUM_CLIENTS)
      ? args.getOptionValue(RoadRunner.OPT_NUM_CLIENTS) : DEFAULT_NUM_CLIENTS;
    String numDocs = args.hasOption(RoadRunner.OPT_NUM_DOCS)
      ? args.getOptionValue(RoadRunner.OPT_NUM_DOCS) : DEFAULT_NUM_DOCS;
    String ratio = args.hasOption(RoadRunner.OPT_RATIO)
      ? args.getOptionValue(RoadRunner.OPT_RATIO) : DEFAULT_RATIO;
    String sampling = args.hasOption(RoadRunner.OPT_SAMPLING)
      ? args.getOptionValue(RoadRunner.OPT_SAMPLING) : DEFAULT_SAMPLING;
    String workload = args.hasOption(RoadRunner.OPT_WORKLOAD)
      ? args.getOptionValue(RoadRunner.OPT_WORKLOAD) : DEFAULT_WORKLOAD;
    String ramp = args.hasOption(RoadRunner.OPT_RAMP)
      ? args.getOptionValue(RoadRunner.OPT_RAMP) : DEFAULT_RAMP;
    String size = args.hasOption(RoadRunner.OPT_DOC_SIZE)
      ? args.getOptionValue(RoadRunner.OPT_DOC_SIZE) : DEFAULT_SIZE;
    String filename = args.hasOption(RoadRunner.OPT_FILENAME) ? args.getOptionValue(RoadRunner.OPT_FILENAME) : null;
    return new GlobalConfig(prepareNodeList(nodes), bucket, password,
      Integer.parseInt(numThreads), Integer.parseInt(numClients),
      Long.parseLong(numDocs), Integer.parseInt(ratio),
      Integer.parseInt(sampling), workload, Integer.parseInt(ramp),
      Integer.parseInt(size), filename);
  }

  /**
   * Converts the node string into a list of URIs.
   *
   * @param nodes The node list as a single string.
   * @return The nodes converted to a list of Strings (one for each node).
   */
  private static List<String> prepareNodeList(final String nodes) {
    return Arrays.asList(nodes.split(","));
  }

  /**
   * @return the nodes
   */
  public List<String> getNodes() {
    return nodes;
  }

  /**
   * @return the bucket
   */
  public String getBucket() {
    return bucket;
  }

  /**
   * @return the password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @return the numThreads
   */
  public int getNumThreads() {
    return numThreads;
  }

  /**
   * @return the numClients
   */
  public int getNumClients() {
    return numClients;
  }

  /**
   * @return the numDocs
   */
  public long getNumDocs() {
    return numDocs;
  }

  /**
   * @return the ratio
   */
  public int getRatio() {
    return ratio;
  }

  /**
   * @return the sampling
   */
  public int getSampling() {
    return sampling;
  }

  /**
   * @return the ramp up time
   */
  public int getRamp() {
    return ramp;
  }

  /**
   * @return the workload
   */
  public String getWorkload() {
    return workload;
  }

  public int getDocumentSize() {
    return size;
  }

  public String getFilename()
  {
    return filename;
  }

  @Override
  public String toString() {
    return "GlobalConfig{" + "nodes=" + nodes + ", bucket=" + bucket
      + ", password=" + password + ", numThreads=" + numThreads
      + ", numClients=" + numClients + ", numDocs=" + numDocs
      + ", ratio=" + ratio + ", sampling=" + sampling + ", workload="
      + workload + ", ramp=" + ramp + ", doc-size=" + size + ", data-filename=" + filename + '}';
  }
}
