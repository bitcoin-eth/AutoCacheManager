package com.demo.cache.script

import org.scalatest.funsuite.AnyFunSuite

class SqlScriptSplitterSpec extends AnyFunSuite {

  test("split removes line comments and returns non-empty statements") {
    val sql =
      """-- prepare source
        |create temp view v1 as select 1 as id;
        |
        |select * from v1; -- final query
        |""".stripMargin

    val statements = SqlScriptSplitter.split(sql)

    assert(statements === Seq(
      "create temp view v1 as select 1 as id",
      "select * from v1"
    ))
  }
}
