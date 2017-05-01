package com.nyu.summary

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.{FlatSpec, FunSuite, Matchers}

/**
  * Created by wzhao on 4/30/17.
  */
class DeDuplicationMethodTest extends FlatSpec with Matchers {

  "withInOneMin" should "determine if two timestamps are within 60s" in {
    val ts1 = 1450631450
    val ts2 = 1450631450
    val ts3 = 1450631466
    val ts4 = 1450631550

    DeDuplication.withInOneMin(ts1, ts2) should equal (true)
    DeDuplication.withInOneMin(ts1, ts3) should equal (true)
    DeDuplication.withInOneMin(ts1, ts4) should equal (false)
  }

  "DeDupHelper" should "remove reduplicated events from events with same advertiser, user and type" in {
    val cba = CollectionByAdUser(1, "123", "click", List(1450631450, 1450631452, 1450631466))

    DeDuplication.deDupHelper(cba) should equal (CollectionByAdUser(1, "123", "click", List(1450631450)))
  }

}
