RoadRunner: a simple Couchbase Server workload generator for Java
==================================================================

RoadRunner is a workload generator written in Java for Couchbase Server. It is intended to be used as a standalone jar. It provides support for customizing lots of aspects, like number of threads per CouchbaseClient, number of CouchbaseClients, amount of docs to store and so on.

Running the `jar` with `-h` shows the supported options:

```
$ java -jar target/RoadRunner-1.0.jar -h
usage: roadrunner
 -b,--bucket <arg>        Name of the bucket (default: "default").
 -c,--num-clients <arg>   Number of CouchbaseClient objects (default:
                          "1").
 -d,--num-docs <arg>      Number of documents to work with (default:
                          "1000").
 -h,--help                Print this help message.
 -n,--nodes <arg>         List of nodes to connect, separated with ","
                          (default: "127.0.0.1").
 -p,--password <arg>      Password of the bucket (default: "").
 -r,--ratio <arg>         Ratio - depending on workload (default: "1").
 -t,--num-threads <arg>   Number of worker threads per CouchbaseClient
                          object (default: "1").
```

Download
--------
The easiest way is to download a prebuilt JAR with all dependencies included and run it. Note that currently, since its in development, no full builds are available to download. As soon as it is more or less feature complete, a JAR will be available for download. Until, read on how to build it fruther down the document.

Usage
-----
When used without any argument, it will run against `127.0.0.1`, use the `default` bucket with no password. It will store 1000 documents and run one get after each set (1:1 ratio). The output looks like this:

```
13:50:58.960 [main] INFO  com.couchbase.roadrunner.RoadRunner - Running with Config: GlobalConfig{nodes=[http://127.0.0.1:8091/pools], bucket=default, password=, numThreads=1, numClients=1, numDocs=10, ratio=1}
13:50:58.963 [main] INFO  com.couchbase.roadrunner.RoadRunner - Initializing ClientHandlers.
13:50:59.191 [main] INFO  com.couchbase.roadrunner.RoadRunner - Running Workload.
13:50:59.211 [main] INFO  com.couchbase.roadrunner.RoadRunner - Finished running Workload in 20ms.
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner - #### Percentile (in microseconds) for get:
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -    5%: 341.0
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -   25%: 364.0
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -   50%: 383.5
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -   75%: 386.0
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -   95%: 1702.0
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -   99%: 1702.0
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner - #### Percentile (in microseconds) for set:
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -    5%: 371.0
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -   25%: 385.125
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -   50%: 419.0
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -   75%: 492.5
13:50:59.214 [main] INFO  com.couchbase.roadrunner.RoadRunner -   95%: 2177.5
13:50:59.215 [main] INFO  com.couchbase.roadrunner.RoadRunner -   99%: 2177.5
```
The workload here finished in `20ms`. You can also find the percentile recordings, not that they are in microseconds, not miliseconds! This means that 75% of all set requests finished in 0.49ms and 95% in 2.1ms. This output format is currently not customizable.

Build
-----
The project is a simple maven project, but it has a dependency onto [hist4j](http://code.google.com/p/hist4j/) to calculate the percentiles. This dependency is not in maven, so you need to get it into your local repo. Clone the source, build it and store it into your local `.m2` repo. It will then be picked up during build.

```
michael@daschlbook ~/histogram $ git clone https://github.com/flaptor/hist4j.git
michael@daschlbook ~/histogram $ cd hist4j/
michael@daschlbook ~/histogram/hist4j $ ant jar
michael@daschlbook ~/histogram/hist4j $ mvn install:install-file -Dfile=output/hist4j-trunk.jar -DgroupId=com.flaptor -DartifactId=hist4j -Dversion=1.0 -Dpackaging=jar
...
[INFO] Installing /Users/michael/histogram/hist4j/output/hist4j-trunk.jar to /Users/michael/.m2/repository/com/flaptor/hist4j/1.0/hist4j-1.0.jar
...
```

Then, clone the RoadRunner project and build it through maven:
```
michael@daschlbook ~/couchbase/RoadRunner $ mvn package
```
You'll find the corresponding JAR files in the `target` directory. The large one (few megs) is with all dependencies included, ready to be distributed. If you want to use the other one, make sure you have all libs in your classpath!
