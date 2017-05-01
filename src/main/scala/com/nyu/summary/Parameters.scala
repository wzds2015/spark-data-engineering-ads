package com.nyu.summary

import com.nyu.summary.Parameters._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by Wenliang Zhao on 4/30/17.
  */

// case classes
case class Event(ts: TimeStamp, eid: EventId, aid: AdvertizerId, uid: UserId, etype: EventType)
case class Impression(ts: TimeStamp, aid: AdvertizerId, cid: CreativeId, uid: UserId)
case class GeneralRecord(ts: TimeStamp, aid: AdvertizerId, uid: UserId, etype: EventType)
case class CollectionByAdUser(aid: AdvertizerId, uid: UserId, etype: EventType, tsList: Seq[TimeStamp])
case class GeneralRecord1(ts: TimeStamp, aid: AdvertizerId, uid: UserId, etype: EventType)
case class GeneralRecordAgg(aid: AdvertizerId, uid: UserId, tsList: Seq[TimeStamp], etypeList: Seq[EventType])
case class GeneralRecordMap(aid: AdvertizerId, uid: UserId, mp: Map[EventType, Int])
case class GeneralAtrribute(aid: AdvertizerId, uid: UserId, etype: EventType, cnt: Int)
case class GeneralAttrWithoutType(aid: AdvertizerId, uid: UserId, cnt: Int)
case class AttributeCountBig(aid: AdvertizerId, etype: EventType, cnt: BigInt)
case class AttributeCount(aid: AdvertizerId, etype: EventType, cnt: Int)
case class UniqueUserList(aid: AdvertizerId, etype: EventType, uidList: Seq[UserId])
case class UniqueUserCount(aid: AdvertizerId, etype: EventType, cnt: Int)


object Parameters {
  // Customized Types
  type TimeStamp = Int
  type EventId = String
  type AdvertizerId = Int
  type UserId = String
  type EventType = String
  type CreativeId = Int

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
