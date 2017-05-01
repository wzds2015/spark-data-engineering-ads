package com.nyu.summary

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

/**
  * Created by Wenliang Zhao on 4/30/17.
  */
class PipelineIntegrationTest extends FunSuite with DatasetSuiteBase {
  test("Test on Whole Pipeline") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val eventDs: Dataset[Event] = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .schema(Parameters.eventSchema)
      .load(Parameters.eventTestFile)
      .as[Event]
    val impressionDs: Dataset[Impression] = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .schema(Parameters.impressionSchema)
      .load(Parameters.impressionTestFile)
      .as[Impression]

    val (attrCountDs, uniqueUserCountDs) = Summarize.pipeline(eventDs, impressionDs, spark)

    val ra1 = AttributeCount(0, "click", 59)
    val ra2 = AttributeCount(0, "visit", 63)
    val ra3 = AttributeCount(0, "purchase", 45)
    val ra4 = AttributeCount(1, "click", 56)
    val ra5 = AttributeCount(1, "visit", 60)
    val ra6 = AttributeCount(1, "purchase", 59)
    val ra7 = AttributeCount(2, "click", 51)
    val ra8 = AttributeCount(2, "visit", 64)
    val ra9 = AttributeCount(2, "purchase", 53)

    val ru1 = UniqueUserCount(0, "click", 10)
    val ru2 = UniqueUserCount(0, "visit", 10)
    val ru3 = UniqueUserCount(0, "purchase", 10)
    val ru4 = UniqueUserCount(1, "click", 10)
    val ru5 = UniqueUserCount(1, "visit", 10)
    val ru6 = UniqueUserCount(1, "purchase", 10)
    val ru7 = UniqueUserCount(2, "click", 10)
    val ru8 = UniqueUserCount(2, "visit", 10)
    val ru9 = UniqueUserCount(2, "purchase", 10)

    val resultAttr = sc.parallelize(List(ra1, ra2, ra3, ra4, ra5, ra6, ra7, ra8, ra9)).toDS.sort("aid", "etype")
    val resultUser = sc.parallelize(List(ru1, ru2, ru3, ru4, ru5, ru6, ru7, ru8, ru9)).toDS.sort("aid", "etype")

    assertDatasetEquals(attrCountDs.sort("aid", "etype"), resultAttr)
    assertDatasetEquals(uniqueUserCountDs.sort("aid", "etype"), resultUser)
  }
}
