package com.nyu.summary

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.FunSuite

/**
  * Created by wzhao on 4/30/17.
  */
class AttributionDatasetTest extends FunSuite with DatasetSuiteBase {
  test("Test Impression to Event") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val imp1 = Impression(0, 0, 0, "a")
    val imp2 = Impression(1, 0, 1, "b")
    val impressionDs = sc.parallelize(List(imp1, imp2)).toDS
    val result = Attribution.impressionToEvent(impressionDs, spark).sort("ts")

    val r1 = GeneralRecord1(0, 0, "a", "impression")
    val r2 = GeneralRecord1(1, 0, "b", "impression")
    val correctResult = sc.parallelize(List(r1, r2)).toDS.sort("ts")

    assertDatasetEquals(result, correctResult)
  }

  test("Test Build Attributed Dataset") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val e1 = GeneralRecord1(1, 1, "a", "click")
    val e2 = GeneralRecord1(2, 1, "a", "impression")
    val e3 = GeneralRecord1(3, 1, "a", "visit")
    val e4 = GeneralRecord1(4, 1, "a", "impression")
    val e5 = GeneralRecord1(5, 1, "a", "click")
    val e6 = GeneralRecord1(6, 1, "a", "click")
    val e7 = GeneralRecord1(7, 1, "a", "purchase")
    val e8 = GeneralRecord1(8, 2, "b", "impression")
    val e9 = GeneralRecord1(9, 2, "b", "impression")
    val e10 = GeneralRecord1(10, 2, "b", "purchase")
    val records = sc.parallelize(List(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10)).toDS

    val result = Attribution.buildAttrDs(records, spark).sort("aid", "uid", "etype")

    val r1 = GeneralAtrribute(1, "a", "click", 1)
    val r2 = GeneralAtrribute(1, "a", "purchase", 0)
    val r3 = GeneralAtrribute(1, "a", "visit", 1)
    val r4 = GeneralAtrribute(2, "b", "click", 0)
    val r5 = GeneralAtrribute(2, "b", "purchase", 1)
    val r6 = GeneralAtrribute(2, "b", "visit", 0)
    val correctResult = sc.parallelize(List(r1, r2, r3, r4, r5, r6)).toDS.sort("aid", "uid", "etype")

    assertDatasetEquals(result, correctResult)
  }
}
