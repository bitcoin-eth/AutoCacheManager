package com.demo.cache.core

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PlanFingerprintSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("PlanFingerprintSpec")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._
    Seq((1, "a"), (2, "b")).toDF("id", "name").createOrReplaceTempView("people")
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("same logical query shape produces the same fingerprint") {
    val leftPlan = spark.sql("select id from people where id > 1").queryExecution.analyzed
    val rightPlan = spark.sql("select id from people where id > 1").queryExecution.analyzed

    assert(PlanFingerprint.of(leftPlan) === PlanFingerprint.of(rightPlan))
  }

  test("different logical query shape produces a different fingerprint") {
    val leftPlan = spark.sql("select id from people where id > 1").queryExecution.analyzed
    val rightPlan = spark.sql("select name from people where id > 1").queryExecution.analyzed

    assert(PlanFingerprint.of(leftPlan) !== PlanFingerprint.of(rightPlan))
  }
}
