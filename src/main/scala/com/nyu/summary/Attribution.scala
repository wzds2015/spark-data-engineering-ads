package com.nyu.summary

import com.nyu.summary.Parameters.{EventType, TimeStamp}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


/**
  * Created by Wenliang Zhao on 4/29/17.
  */

object Attribution {
  /**
    *
    * @param impDs   original impression dataset
    * @param spark   spark session
    * @return        change impression to event format
    */
  def impressionToEvent(impDs: Dataset[Impression], spark: SparkSession) : Dataset[GeneralRecord1] = {
    import spark.implicits._
    impDs.map(imp => GeneralRecord1(imp.ts, imp.aid, imp.uid, "impression"))
  }

  /**
    *
    * @param eventDs  event dataset (general form)
    * @param impDs    impression dataset (general form)
    * @return         combine above two
    */
  def mergeRecords(eventDs: Dataset[GeneralRecord1], impDs: Dataset[GeneralRecord1])
      : Dataset[GeneralRecord1] = {
    eventDs union impDs
  }

  /**
    *
    * @param tsList      timestamp list of one aid and uid
    * @param etypeList   event type list
    * @return            a Map storing attribution count for each event type
    */
  def buildAttr(tsList: Seq[TimeStamp], etypeList: Seq[EventType])
      : Map[EventType, Int] = {
    val records = (tsList zip etypeList).sortWith(_._1 < _._1).map(_._2)
    // ToDo: if final count is 0, still stored in the Map, takes extra spaces need to further investigation, which needs more condition check but less space
    // In this test, this is not a problem because the data is dense (have counts on every event types)
    val eventCount = Map("click"->0, "visit"->0, "purchase"->0)
    val comb = records.length match {
      case 1 => (eventCount, "")
      case _ => records.tail.foldLeft((eventCount, records.head))({ case ((ec: Map[EventType, Int], prev: EventType), curr: EventType)
      => (prev, curr) match {
        case ("impression", s) => s match {
          case "impression" => (ec, curr)
          case _ => (ec + (s -> (ec(s) + 1)), s)
        }
        case _ => (ec, curr)
      }
      })
    }
    comb._1
  }

  /**
    *
    * @param records   big dataset in general event form with both events and impressions
    * @param spark     spark session
    * @return          dataset whose row is one aid, uid, etype with the attribution counts accordingly
    */
  def buildAttrDs(records: Dataset[GeneralRecord1], spark: SparkSession) : Dataset[GeneralAtrribute] = {
    import spark.implicits._
    records.groupBy("aid", "uid").agg(collect_list(col("ts")) as "tsList", collect_list(col("etype")) as "etypeList")
           .as[GeneralRecordAgg]
           .map(gra => GeneralRecordMap(gra.aid, gra.uid, buildAttr(gra.tsList, gra.etypeList)))
           .flatMap(grm => grm.mp.map(tp => GeneralAtrribute(grm.aid, grm.uid, tp._1, tp._2)))
  }

  /**
    *
    * @param attrDs  dataset whose row is one aid, uid, etype with the attribution counts accordingly
    * @param spark   spark session
    * @return        total attribution count for each aid and etype
    */
  def countAttrByAdAndEtype(attrDs: Dataset[GeneralAtrribute], spark: SparkSession) : Dataset[AttributeCount] = {
    import spark.implicits._
    attrDs.groupBy("aid", "etype").agg(sum(col("cnt")) as "cnt")
      .as[AttributeCountBig]
      .map(acb => AttributeCount(acb.aid, acb.etype, acb.cnt.toInt))
  }

  /**
    *
    * @param attrDs  dataset whose row is one aid, uid, etype with the attribution counts accordingly
    * @param spark   spark session
    * @return        unique user count for each aid and etype
    */
  def countUniqueUser(attrDs: Dataset[GeneralAtrribute], spark: SparkSession) : Dataset[UniqueUserCount] = {
    import spark.implicits._
    // ToDo: collect_set is lack of encoder
    attrDs.filter(ga => ga.cnt!=0).groupBy("aid", "etype").agg(collect_list(col("uid")) as "uidList")
          .as[UniqueUserList].map(uus => UniqueUserCount(uus.aid, uus.etype, uus.uidList.distinct.size))
  }

}
