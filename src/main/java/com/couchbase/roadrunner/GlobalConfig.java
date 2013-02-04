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

import java.net.URI;
import java.util.ArrayList;
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
final class GlobalConfig {

  /** Configure a reusable logger. */
  static final Logger LOGGER =
    LoggerFactory.getLogger(GlobalConfig.class.getName());

  private static final String DEFAULT_NODES = "127.0.0.1";
  private static final String DEFAULT_BUCKET = "default";
  private static final String DEFAULT_PASSWORD = "";
  private static final String DEFAULT_NUM_THREADS = "1";
  private static final String DEFAULT_NUM_CLIENTS = "1";
  private static final String DEFAULT_NUM_DOCS = "1000";
  private static final String DEFAULT_RATIO = "1";

  private final List<URI> nodes;
  private final String bucket;
  private final String password;
  private final int numThreads;
  private final int numClients;
  private final long numDocs;
  private final int ratio;

  /**
   * Create the GlobalConfig.
   *
   * @param nodes The list of nodes, separated by ",".
   * @param bucket The name of the bucket.
   * @param password The password of the bucket.
   * @param numThreads The number of threads.
   * @param numClients The number of CouchbaseClients.
   */
  private GlobalConfig(List<URI> nodes, String bucket, String password,
    int numThreads, int numClients, long numDocs, int ratio) {
    this.nodes = Collections.unmodifiableList(nodes);
    this.bucket = bucket;
    this.password = password;
    this.numThreads = numThreads;
    this.numClients = numClients;
    this.numDocs = numDocs;
    this.ratio = ratio;
  }

  /**
   * Create the GlobalConfig object by parsing the command line args and
   * applying defaults.
   *
   * @param args The passed in command line arguments.
   * @return A immutable GlobalConfig object.
   */
  public static GlobalConfig fromCommandLine(final CommandLine args) {
    String nodes = args.hasOption("nodes")
      ? args.getOptionValue("nodes") : DEFAULT_NODES;
    String bucket = args.hasOption("bucket")
      ? args.getOptionValue("bucket") : DEFAULT_BUCKET;
    String password = args.hasOption("password")
      ? args.getOptionValue("password") : DEFAULT_PASSWORD;
    String numThreads = args.hasOption("num-threads")
      ? args.getOptionValue("num-threads") : DEFAULT_NUM_THREADS;
    String numClients = args.hasOption("num-clients")
      ? args.getOptionValue("num-clients") : DEFAULT_NUM_CLIENTS;
    String numDocs = args.hasOption("num-docs")
      ? args.getOptionValue("num-docs") : DEFAULT_NUM_DOCS;
    String ratio = args.hasOption("ratio")
      ? args.getOptionValue("ratio") : DEFAULT_RATIO;
    return new GlobalConfig(prepareNodeList(nodes), bucket, password,
      Integer.parseInt(numThreads), Integer.parseInt(numClients),
      Long.parseLong(numDocs), Integer.parseInt(ratio));
  }

  /**
   * Converts the node string into a list of URIs.
   *
   * @param nodes The node list as a string.
   * @return The converted list of URIs.
   */
  private static List<URI> prepareNodeList(final String nodes) {
    List<String> splitNodes = Arrays.asList(nodes.split(","));
    List<URI> converted = new ArrayList<URI>();
    try {
      for (String node : splitNodes) {
        converted.add(new URI("http://" + node + ":8091/pools"));
      }
    } catch (Exception ex) {
      LOGGER.error("Could not parse node list: " + ex);
      System.exit(-1);
    }
    return converted;
  }

  /**
   * @return the nodes
   */
  public List<URI> getNodes() {
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

  @Override
  public String toString() {
    return "GlobalConfig{" + "nodes=" + nodes + ", bucket=" + bucket
      + ", password=" + password + ", numThreads=" + numThreads
      + ", numClients=" + numClients + ", numDocs=" + numDocs
      + ", ratio=" + ratio + '}';
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
}
