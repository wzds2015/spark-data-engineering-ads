package com.nyu.summary

import com.nyu.summary.Parameters.TimeStamp
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Wenliang Zhao on 4/28/17.
  */

object DeDuplication {
  /**
    *
    * @param i1  timestamp
    * @param i2  timestamp
    * @return    if the second timestamp is within 1 min of the first one
    */
  def withInOneMin(i1: TimeStamp, i2: TimeStamp) : Boolean = (i2 - i1 <= 60)

  /**
    *
    * @param cba  an instance of CollectionByAdUser (last member is a list of ts)
    * @return     remove duplicates in the timestamp list
    */
  def deDupHelper(cba: CollectionByAdUser) : CollectionByAdUser = {
    val tsList = cba.tsList.sortWith(_ < _)
    tsList.length match {
      case 1 => cba
      case _ => CollectionByAdUser(cba.aid, cba.uid, cba.etype,
                tsList.tail.foldLeft(List(tsList.head))({case (l: List[TimeStamp], ts: TimeStamp)
                    => {
                  if (withInOneMin(l.head, ts)) l
                  else ts :: l
                }})
      )
    }
  }

  /**
    *
    * @param eventDs  original event dataset
    * @param spark    spark session
    * @return         remove duplicated timestamp wihtin each group (group based on aid, uid, etype)
    */
  def deDuplicate(eventDs: Dataset[Event], spark: SparkSession) : Dataset[GeneralRecord1] = {
    import spark.implicits._
    eventDs.groupBy("aid", "uid", "etype")
           .agg(collect_list(col("ts")) as "tsList")
           .as[CollectionByAdUser]
           .map(cba => deDupHelper(cba))
           .flatMap(cba => cba.tsList.map(
             ts => GeneralRecord1(ts, cba.aid, cba.uid, cba.etype)
           ))
  }
}
