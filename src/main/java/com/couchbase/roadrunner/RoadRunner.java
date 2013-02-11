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

import com.flaptor.hist4j.AdaptiveHistogram;
import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RoadRunner project is a load tester for your Couchbase cluster.
 *
 * With this load tester, you can specify the number for CouchbaseClients and
 * corresponding worker threads to execute a workload against the connected
 * cluster. In addition to producing raw workload, it is able to measure
 * latency and throughput, therefore giving you a tool to measure the
 * performance of your cluster and JVM environment by testing it in isolation.
 * You can use it for both performance and debugging purposes.
 *
 * By default, it connects to localhost, using the "default" bucket with no
 * password. Also, it uses one CouchbaseClient and one worker thread. You can
 * change the behavior by providing custom command line arguments. Use the
 * "-h" flag to see all available options.
 */
public final class RoadRunner {

  /** Configure a reusable logger. */
  private static final Logger LOGGER =
    LoggerFactory.getLogger(RoadRunner.class.getName());

  /** Do not use a public constructor for the main class. */
  private RoadRunner() { }

  /**
   * Initialize the RoadRunner.
   *
   * This method is responsible for parsing the passed in command line arguments
   * and also dispatch the bootstrapping of the actual workload runner.
   *
   * @param args Command line arguments to be passed in.
   */
  public static void main(final String[] args) {
    CommandLine params = null;
    try {
      params = parseCommandLine(args);
    } catch (ParseException ex) {
      LOGGER.error("Exception while parsing command line!", ex);
      System.exit(-1);
    }

    Properties systemProperties = System.getProperties();
    systemProperties.put("net.spy.log.LoggerImpl",
      "net.spy.memcached.compat.log.SunLogger");
    System.setProperties(systemProperties);
    java.util.logging.Logger.getLogger("com.couchbase.client")
      .setLevel(Level.WARNING);

    if (params.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("roadrunner", getCommandLineOptions());
      System.exit(0);
    }

    GlobalConfig config = GlobalConfig.fromCommandLine(params);
    WorkloadDispatcher dispatcher = new WorkloadDispatcher(config);

    LOGGER.info("Running with Config: " + config.toString());

    try {
      LOGGER.info("Initializing ClientHandlers.");
      dispatcher.init();
    } catch (Exception ex) {
      LOGGER.error("Error while initializing the ClientHandlers: ", ex);
      System.exit(-1);
    }

    Stopwatch workloadStopwatch = new Stopwatch().start();
    try {
      LOGGER.info("Running Workload.");
      dispatcher.dispatchWorkload();
    } catch (Exception ex) {
      LOGGER.error("Error while running the Workload: ", ex);
      System.exit(-1);
    }
    workloadStopwatch.stop();

    LOGGER.info("Finished running Workload.");

    dispatcher.prepareMeasures();
    Map<String, List<Stopwatch>> measures = dispatcher.getMeasures();
    for (Map.Entry<String, List<Stopwatch>> entry : measures.entrySet()) {
      AdaptiveHistogram histogram = new AdaptiveHistogram();
      for (Stopwatch watch : entry.getValue()) {
        histogram.addValue(
          (long)(Math.round(watch.elapsed(TimeUnit.MICROSECONDS) * 100)/100));
      }

      LOGGER.info("#### Percentile (in microseconds) for "+entry.getKey()+":");
      LOGGER.info("   5%: " + histogram.getValueForPercentile(5));
      LOGGER.info("  25%: " + histogram.getValueForPercentile(25));
      LOGGER.info("  50%: " + histogram.getValueForPercentile(50));
      LOGGER.info("  75%: " + histogram.getValueForPercentile(75));
      LOGGER.info("  95%: " + histogram.getValueForPercentile(95));
      LOGGER.info("  99%: " + histogram.getValueForPercentile(99));
    }

    long totalOps = config.getNumDocs();
    long opsPerSecond = (long) (((totalOps*0.1)
      / workloadStopwatch.elapsed(TimeUnit.MILLISECONDS)) * 10000);
    LOGGER.info("#### Total Ops: " + totalOps +", Elapsed: "
      + workloadStopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
    LOGGER.info("#### That is around " + opsPerSecond + "ops/s.");
  }

  /**
   * Parse the command line.
   *
   * @param args Command line arguments to be passed in.
   * @return The parsed command line options.
   * @throws ParseException Thrown when the command line could not be parsed.
   */
  private static CommandLine parseCommandLine(final String[] args)
    throws ParseException {
    CommandLineParser parser = new PosixParser();
    CommandLine params =  parser.parse(getCommandLineOptions(), args);
    return params;
  }

  /**
   * Defines the default command line options.
   *
   * @return Supported command line options.
   */
  private static Options getCommandLineOptions() {
    Options options = new Options();
    options.addOption("n", "nodes", true,
      "List of nodes to connect, separated with \",\" (default: "
      + "\"127.0.0.1\").");
    options.addOption("b", "bucket", true,
      "Name of the bucket (default: \"default\").");
    options.addOption("p", "password", true,
      "Password of the bucket (default: \"\").");
    options.addOption("t", "num-threads", true,
      "Number of worker threads per CouchbaseClient object (default: \"1\").");
    options.addOption("c", "num-clients", true,
      "Number of CouchbaseClient objects (default: \"1\").");
    options.addOption("d", "num-docs", true,
      "Number of documents to work with (default: \"1000\").");
    options.addOption("r", "ratio", true,
      "Ratio - depending on workload (default: \"1\").");
    options.addOption("h", "help", false,
      "Print this help message.");
    options.addOption("s", "sampling", true, "% Sample Rate (default 100%)");
    return options;
  }
}
