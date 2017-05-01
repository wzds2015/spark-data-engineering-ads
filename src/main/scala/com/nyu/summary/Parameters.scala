package com.nyu.summary

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by wzhao on 4/30/17.
  */

// case classes
case class Event(ts: Int, eid: String, aid: Int, uid: String, etype: String)
case class Impression(ts: Int, aid: Int, cid: Int, uid: String)
case class GeneralRecord(ts: Int, aid: Int, uid: String, etype: String)
case class CollectionByAdUser(aid: Int, uid: String, etype: String, tsList: Seq[Int])
case class GeneralRecord1(ts: Int, aid: Int, uid: String, etype: String)
case class GeneralRecordAgg(aid: Int, uid: String, tsList: Seq[Int], etypeList: Seq[String])
case class GeneralRecordMap(aid: Int, uid: String, mp: Map[String, Int])
case class GeneralAtrribute(aid: Int, uid: String, etype: String, cnt: Int)
case class GeneralAttrWithoutType(aid: Int, uid: String, cnt: Int)
case class AttributeCountBig(aid: Int, etype: String, cnt: BigInt)
case class AttributeCount(aid: Int, etype: String, cnt: Int)
case class UniqueUserList(aid: Int, etype: String, uidList: Seq[String])
case class UniqueUserCount(aid: Int, etype: String, cnt: Int)



object Parameters {
  val eventSchema = StructType(Array(
    StructField("ts", IntegerType, false),
    StructField("eid", StringType, false),
    StructField("aid", IntegerType, false),
    StructField("uid", StringType, false),
    StructField("etype", StringType, false)
  ))

  val impressionSchema = StructType(Array(
    StructField("ts", IntegerType, false),
    StructField("aid", IntegerType, false),
    StructField("cid", IntegerType, false),
    StructField("uid", StringType)
  ))

  val eventTestFile = "data/events.csv"
  val impressionTestFile = "data/impressions.csv"

  val attrOutFile = "output/attrCount.csv"
  val userOutFile = "output/userCount.csv"
}
