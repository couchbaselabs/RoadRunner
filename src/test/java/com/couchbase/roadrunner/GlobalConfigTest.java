package com.couchbase.roadrunner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GlobalConfigTest
{
    private static int NUM_THREADS = 97;
    private static int NUM_CLIENTS = 55;
    private static int NUM_DOCS = 1234;
    private static int RATIO_INT = 76;
    private static int SAMPLING_RATE = 99;
    private static int RAMP_TIME = 13;
    private static int DOC_SIZE = 5678;

    private static final String NODE = "1.2.3.4";
    private static final String BUCKET = "testBucket";
    private static final String PASSWORD = "testPassword";
    private static final String THREADS = "" + NUM_THREADS;
    private static final String CLIENTS = "" + NUM_CLIENTS;
    private static final String DOCS = "" + NUM_DOCS;
    private static final String RATIO = "" + RATIO_INT;
    private static final String SAMPLING = "" + SAMPLING_RATE;
    private static final String WORKLOAD = "testWorkload";
    private static final String RAMP = "" + RAMP_TIME;
    private static final String SIZE = "" + DOC_SIZE;

    private static final String NODE_POOLS = "http://" + NODE + ":8091/pools";

    @Test
    public void testShortOptions() throws ParseException
    {
        String [] args = new String[]{//
                        // as received by main method
                        "-n", NODE, //
                        "-b", BUCKET, //
                        "-p", PASSWORD, //
                        "-t", THREADS, //
                        "-c", CLIENTS, //
                        "-d", DOCS, //
                        "-R", RATIO, //
                        "-s", SAMPLING, //
                        "-w", WORKLOAD, //
                        "-r", RAMP, //
                        "-S", SIZE //
                        };
        CommandLine parsed = RoadRunner.parseCommandLine(args);

        GlobalConfig config = GlobalConfig.fromCommandLine(parsed);
        Assert.assertEquals(config.getNodes().size(), 1, "number of nodes detected");
        Assert.assertEquals(config.getNodes().get(0).toString(), NODE_POOLS, "registered node");
        Assert.assertEquals(config.getBucket(), BUCKET, "bucket name");
        Assert.assertEquals(config.getPassword(), PASSWORD, "password");
        Assert.assertEquals(config.getNumThreads(), NUM_THREADS, "number of threads");
        Assert.assertEquals(config.getNumClients(), NUM_CLIENTS, "number of clients");
        Assert.assertEquals(config.getNumDocs(), NUM_DOCS, "number of docs");
        Assert.assertEquals(config.getRatio(), RATIO_INT, "ratio");
        Assert.assertEquals(config.getSampling(), SAMPLING_RATE, "sampling");
        Assert.assertEquals(config.getWorkload(), WORKLOAD, "workload");
        Assert.assertEquals(config.getRamp(), RAMP_TIME, "ramp");
        Assert.assertEquals(config.getDocumentSize(), DOC_SIZE, "doc size");
    }
}
