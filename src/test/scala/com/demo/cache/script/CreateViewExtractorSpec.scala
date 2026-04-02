package com.demo.cache.script

import org.scalatest.funsuite.AnyFunSuite

class CreateViewExtractorSpec extends AnyFunSuite {

  test("extract recognizes create view and final query units") {
    val statements = Seq(
      "create or replace temp view demo.v_orders as select id, amount from orders",
      "with t as (select * from demo.v_orders) select count(*) from t"
    )

    val units = CreateViewExtractor.extract(statements)

    assert(units.size === 2)
    assert(units.head.sourceType === "CREATE_VIEW")
    assert(units.head.objectName.contains("demo.v_orders"))
    assert(units.head.queryText === "select id, amount from orders")

    assert(units(1).sourceType === "FINAL_QUERY")
    assert(units(1).objectName.isEmpty)
  }

  test("extract ignores unsupported statements") {
    val statements = Seq(
      "set spark.sql.shuffle.partitions=8",
      "drop view if exists old_view"
    )

    assert(CreateViewExtractor.extract(statements).isEmpty)
  }
}
