package com.nyu.summary

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by wzhao on 4/30/17.
  */
class AttributionMethodTest extends FlatSpec with Matchers {
  "buildAttr" should "aggregate count for attribution of every event type and store into a Map" in {
    val tsList = List(2, 4, 1, 3, 5, 7, 6)
    val etypeList = List("impression", "impression", "click", "visit", "click", "purchase", "click")

    Attribution.buildAttr(tsList, etypeList) should equal (Map("visit"->1, "click"->1, "purchase"->0))
  }
}
