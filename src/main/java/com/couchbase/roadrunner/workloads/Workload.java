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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;

public abstract class Workload implements Runnable {

  /** Configure a reusable logger. */
  private final Logger logger =
    LoggerFactory.getLogger(Workload.class.getName());

  /** Reference to the Couchbase Bucket */
  private final Bucket bucket;

  /** Name of the Workload */
  private final String workloadName;

  /** Counter of total measured ops */
  private long measuredOps;

  /** Counter of total ops */
  private long totalOps;

  /** Ramp time */
  private long ramp;

  /** Total runtime of this workload thread */
  private Stopwatch elapsed;

  /** Measures */
  private Map<String, List<Stopwatch>> measures;

  private final DocumentFactory documentFactory;

  public Workload(final Bucket bucket, final String name,
    final int ramp, final DocumentFactory documentFactory) {
    this.bucket = bucket;
    this.workloadName = name;
    this.measures = new HashMap<String, List<Stopwatch>>();
    this.measuredOps = 0;
    this.totalOps = 0;
    this.ramp = ramp;
    this.elapsed = new Stopwatch();
    this.documentFactory = documentFactory;
  }

  public long getTotalOps() {
    return totalOps;
  }

  public void incrTotalOps() {
    totalOps++;
  }

  public void startTimer() {
    elapsed.start();
  }

  public void endTimer() {
    elapsed.stop();
  }

  /**
   * Store a measure for later retrieval.
   *
   * If the ramp-up time is not yet through, don't measure the
   * operation.
   *
   * @param identifier Identifier of the stopwatch.
   * @param watch The stopwatch.
   */
  public void addMeasure(String identifier, Stopwatch watch) {
    if (elapsed.elapsed(TimeUnit.SECONDS) < ramp) {
      return;
    }

    if(!measures.containsKey(identifier)) {
      measures.put(identifier, new ArrayList<Stopwatch>());
    }
    measures.get(identifier).add(watch);
    measuredOps++;
  }

  public Map<String, List<Stopwatch>> getMeasures() {
    return measures;
  }

  public long getMeasuredOps() {
    return measuredOps;
  }

  public Stopwatch totalElapsed() {
    if(elapsed.isRunning()) {
      throw new IllegalStateException("Stopwatch still running!");
    }
    return elapsed;
  }

  /**
   * @return the bucket
   */
  protected Bucket getBucket() {
    return bucket;
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
    return logger;
  }

  public String randomKey() {
    return UUID.randomUUID().toString();
  }

  protected SampleDocument getDocument() {
    return documentFactory.getDocument();
  }

  static interface SampleDocument{}

  /**
   * This document consists entirely of random bytes.
   */
  static class RandomDocument implements Serializable, SampleDocument {

    private static final long serialVersionUID = 974277240501163457L;
    public final byte[] payload;

    public RandomDocument(int payloadSize) {
      byte[] bytes = new byte[payloadSize];
      new Random().nextBytes(bytes);
      this.payload = bytes;
    }
  }

  /**
   * Reads a file from disk and uses the contents as the document to be stored.
   * Each line of the file is trimmed and concatenated down to a single string.
   * This allows a "friendly" json/xml/other document with formatting to be
   * given even though the internal representation would not be formatted.
   *
   * @author bvesco, May 21, 2013
   */
  static class FileReaderDocument implements Serializable, SampleDocument {

    private static final long serialVersionUID = 1612506081910846384L;
    public final byte[] payload;

    public FileReaderDocument(String filename) throws IOException
    {
      File file = new File(filename);
      List<String> lines = Files.readLines(file, Charsets.UTF_8);
      StringBuilder sb = new StringBuilder();
      for (String line : lines)
      {
        sb.append(line.trim());
      }
      payload = sb.toString().getBytes();
    }
  }

  public static interface DocumentFactory{
    SampleDocument getDocument();
  }

  /**
   * Generates documents of a fixed size but each document has a random sequence
   * of bytes.
   *
   * @author bvesco, May 21, 2013
   */
  public static class FixedSizeRandomDocumentFactory implements DocumentFactory {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final int sizeInBytes;

    public FixedSizeRandomDocumentFactory(int sizeInBytes){
      this.sizeInBytes = sizeInBytes;
      logger.info("Factory using document size of {} bytes", this.sizeInBytes);
    }

    @Override
    public SampleDocument getDocument()
    {
        return new RandomDocument(sizeInBytes);
    }
  }

  /**
   * Generates documents with bytes that were read from a file. The same
   * document is returned every time.
   *
   * @author bvesco, May 21, 2013
   */
  public static class SingleFileDocumentFactory implements DocumentFactory {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final FileReaderDocument document;

    public SingleFileDocumentFactory(String filename) throws IOException{
      this.document = new FileReaderDocument(filename);
      logger.info("Factory using document size of {} bytes from {}", document.payload.length, filename);
    }

    @Override
    public SampleDocument getDocument()
    {
      return document;
    }
  }
}
