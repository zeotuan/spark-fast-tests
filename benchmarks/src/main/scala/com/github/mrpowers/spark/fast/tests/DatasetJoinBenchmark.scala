package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

private class DatasetJoinBenchmark extends DatasetComparer {
//  def getSparkSession: SparkSession = {
//    val session = SparkSession
//      .builder()
//      .master("local")
//      .appName("spark session")
//      .getOrCreate()
//    session.sparkContext.setLogLevel("ERROR")
//    session
//  }
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.SingleShotTime))
//  @Fork(value = 2)
//  @Warmup(iterations = 10)
//  @Measurement(iterations = 10)
//  @OutputTimeUnit(TimeUnit.NANOSECONDS)
//  def dataframeJoinBenchmark(blackHole: Blackhole): Boolean = {
//    val spark = getSparkSession
//    val ds1   = spark.range(0, 1000000, 1, 8)
//    val ds3   = ds1
//    val joinedDSResult =
//      ds1
//        .as("l")
//        .join(ds3.as("r"), Seq("id"))
//        .filter(ds1.columns.map(k => col(s"l.$k") === col(s"r.$k")).reduce(_ && _))
//
//    blackHole.consume(joinedDSResult.isEmpty)
//    joinedDSResult.isEmpty
//  }
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.SingleShotTime))
//  @Fork(value = 2)
//  @Warmup(iterations = 10)
//  @Measurement(iterations = 10)
//  @OutputTimeUnit(TimeUnit.NANOSECONDS)
//  def dataframeJoinWithBenchmark(blackHole: Blackhole): Boolean = {
//    val spark = getSparkSession
//    val ds1 = spark
//      .range(0, 1000000, 1, 8)
//    val ds3 = ds1
//    val joinedDSResult =
//      ds1
//        .toDF()
//        .as("l")
//        .joinWith(ds3.toDF().as("r"), Seq("id").map(k => col(s"l.$k") === col(s"r.$k")).reduce(_ && _))
//        .filter((p: (Row, Row)) => p._1 == p._2)
//
//    blackHole.consume(joinedDSResult.isEmpty)
//    joinedDSResult.isEmpty
//  }
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.SingleShotTime))
//  @Fork(value = 2)
//  @Warmup(iterations = 10)
//  @Measurement(iterations = 10)
//  @OutputTimeUnit(TimeUnit.NANOSECONDS)
//  def dataframeOuterJoinWithBenchmark(blackHole: Blackhole): Boolean = {
//    val spark = getSparkSession
//    val ds1 = spark
//      .range(0, 1000000, 1, 8)
//    val ds3 = ds1
//    val joinedDSResult =
//      ds1
//        .toDF()
//        .as("l")
//        .outerJoinWith(ds3.toDF().as("r"), Seq("id"))
//        .filter((p: (Option[Row], Option[Row])) => p._1 == p._2)
//
//    blackHole.consume(joinedDSResult.isEmpty)
//    joinedDSResult.isEmpty
//  }
//
//  @Benchmark
//  @BenchmarkMode(Array(Mode.SingleShotTime))
//  @Fork(value = 2)
//  @Warmup(iterations = 10)
//  @Measurement(iterations = 10)
//  @OutputTimeUnit(TimeUnit.NANOSECONDS)
//  def datasetJoinWithBenchmark(blackHole: Blackhole): Boolean = {
//    val spark = getSparkSession
//    val ds1 = spark
//      .range(0, 1000000, 1, 8)
//    val ds3 = ds1
//    val joinedDSResult = ds1
//      .as("l")
//      .joinWith(ds3.as("r"), Seq("id").map(k => col(s"l.$k") === col(s"r.$k")).reduce(_ && _))
//      .filter(p => p._1 == p._2)
//
//    blackHole.consume(joinedDSResult.isEmpty)
//    joinedDSResult.isEmpty
//  }
}
