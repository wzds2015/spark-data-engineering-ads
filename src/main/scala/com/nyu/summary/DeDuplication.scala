package com.nyu.summary

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by admin on 4/28/17.
  */
case class 


object DeDuplication {
  def deDupHelper()

  def deduplicate(eventDs: Dataset[Event], spark: SparkSession) : Dataset[Event] = {
    eventDs.groupBy("aid", "uid")
           .agg(collect_list(col("ts")) as "ts",
                collect_list(col("eid")) as "eid",
                collect_list(col("etype")) as "etype"
           )

  }
}
