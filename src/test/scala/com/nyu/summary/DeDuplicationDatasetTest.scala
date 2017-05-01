package com.nyu.summary

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.FunSuite

/**
  * Created by wzhao on 4/30/17.
  */
class DeDuplicationDatasetTest extends FunSuite with DatasetSuiteBase {
  test("Test Deduplication") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val e1 = Event(1450631450, "5bb2b119-226d-4bdf-95ad-a1cdf9659789", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click")
    val e2 = Event(1450631452, "23aa6216-3997-4255-9e10-7e37a1f07060", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click")
    val e3 = Event(1450631464, "61c3ed32-01f9-43c4-8f54-eee3857104cc", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click")
    val e4 = Event(1450631466, "20702cb7-60ca-413a-8244-d22353e2be49", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "purchase")
    val e5 = Event(1450631460, "5bb2b118-226d-4bdf-95ad-a1cdf9659789", 2, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click")
    val events = sc.parallelize(List(e1, e2, e3, e4, e5)).toDS
    val result = DeDuplication.deDuplicate(events, spark).sort("ts")

    val r1 = GeneralRecord1(1450631450, 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click")
    val r2 = GeneralRecord1(1450631466, 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "purchase")
    val r3 = GeneralRecord1(1450631460, 2, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click")
    val correctResult = sc.parallelize(List(r1, r2, r3)).toDS.sort("ts")

    assertDatasetEquals(result, correctResult)
  }
}
