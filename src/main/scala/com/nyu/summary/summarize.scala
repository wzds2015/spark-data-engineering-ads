package com.nyu.summary

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Wenliang Zhao on 4/27/17.
  */

// case classes
case class Event(ts: Int, eid: String, aid: Int, uid: String, etype: String)
case class Impression(ts: Int, aid: Int, cid: Int, uid: String)
case class GeneralRecord(ts: Int, aid: Int, uid: String, etype: String)

object summarize extends App {
  main(args)
  override def main(args: Array[String]) : Unit = {
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
      .builder()
      .appName("User Activity and Segments")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    // spark settings
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions", 2)
    spark.conf.set("spark.driver.memory", "2g")
    spark.conf.set("spark.executor.memory", "2g")
    spark.sparkContext.setLogLevel("WARN")

    val eventFile = args(0)
    val impressionFile = args(1)

    val eventDs: Dataset[Event] = spark.read.format("com.databricks.spark.csv")
          .option("delimiter", ",")
          .load(eventFile)
          .as[Event]
    val impressionDs: Dataset[Impression] = spark.read.format("com.databricks.spark.csv")
          .option("delimiter", ",")
          .load(impressionFile)
          .as[Impression]
  }
}
