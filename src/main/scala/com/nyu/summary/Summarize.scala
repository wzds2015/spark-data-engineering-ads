package com.nyu.summary

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Wenliang Zhao on 4/27/17.
  */

object Summarize extends App {
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

    // imput arguments: paths for event and impression files
    val eventFile = args(0)
    val impressionFile = args(1)

    // original dataset reading
    val eventDs: Dataset[Event] = spark.read.format("com.databricks.spark.csv")
          .option("delimiter", ",")
          .schema(Parameters.eventSchema)
          .load(eventFile)
          .as[Event]
    val impressionDs: Dataset[Impression] = spark.read.format("com.databricks.spark.csv")
          .option("delimiter", ",")
          .schema(Parameters.impressionSchema)
          .load(impressionFile)
          .as[Impression]

    // run pipeline
    val (attrCountDs, uniqueUserCountDs) = pipeline(eventDs, impressionDs, spark)

    // write to disk
    attrCountDs.toDF.write.parquet(Parameters.attrOutFile)
    uniqueUserCountDs.toDF.write.parquet(Parameters.userOutFile)

    // stop spark context
    spark.stop()
  }

  /**
    *
    * @param events      original event dataset
    * @param impression  original impression dataset
    * @param spark       spark session
    * @return            final results
    */
  def pipeline(events: Dataset[Event], impression: Dataset[Impression], spark: SparkSession)
        : (Dataset[AttributeCount], Dataset[UniqueUserCount]) = {
    val merged = originToMerged(events, impression, spark)
    mergedToResult(merged, spark)
  }

  /**
    *
    * @param events       original event dataset
    * @param impression   original impression dataset
    * @param spark        spark session
    * @return             partial pipeline from raw to merged dataset (event and impression)
    */
  def originToMerged(events: Dataset[Event], impression: Dataset[Impression], spark: SparkSession) : Dataset[GeneralRecord1] = {
    val deDupEvents = DeDuplication.deDuplicate(events, spark)
    val impEvents = Attribution.impressionToEvent(impression, spark)
    Attribution.mergeRecords(deDupEvents, impEvents)
  }

  /**
    *
    * @param records     merged dataset
    * @param spark       spark session
    * @return            final results
    */
  def mergedToResult(records: Dataset[GeneralRecord1], spark: SparkSession)
      : (Dataset[AttributeCount], Dataset[UniqueUserCount]) = {
    val attrDs = Attribution.buildAttrDs(records, spark).persist
    (Attribution.countAttrByAdAndEtype(attrDs, spark), Attribution.countUniqueUser(attrDs, spark))
  }
}
