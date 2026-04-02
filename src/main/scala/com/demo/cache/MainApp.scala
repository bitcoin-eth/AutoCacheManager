package com.demo.cache

import com.demo.cache.core.SubplanCollector
import com.demo.cache.model.{GlobalRepeatedCandidate, QueryUnit}
import com.demo.cache.script.{CreateViewExtractor, SqlScriptSplitter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.io.Source
import java.io.PrintWriter

object MainApp {

  def main(args: Array[String]): Unit = {

    val argMap = parseArgs(args)

    val sqlFile = argMap.getOrElse(
      "sqlFile",
      throw new IllegalArgumentException("Missing --sqlFile")
    )

    val minOccurrences = argMap.get("minOccurrences").map(_.toInt).getOrElse(2)
    val minNodeSize = argMap.get("minNodeSize").map(_.toInt).getOrElse(3)
    val dumpFile = argMap.getOrElse("dumpFile", "repeated_candidates.txt")

    require(minOccurrences >= 2, s"minOccurrences must be >= 2, but got $minOccurrences")
    require(minNodeSize >= 1, s"minNodeSize must be >= 1, but got $minNodeSize")

    val spark = SparkSession.builder()
      .appName("spark-sql-reuse-detector")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val sqlText = readSql(sqlFile)

      val statements = SqlScriptSplitter.split(sqlText)

      println("========== statements ==========")
      statements.zipWithIndex.foreach { case (stmt, idx) =>
        println(s"[${idx + 1}] ${stmt.take(120).replace("\n", " ")}")
      }

      val queryUnits: Seq[QueryUnit] = CreateViewExtractor.extract(statements)

      println("========== extracted query units ==========")
      if (queryUnits.isEmpty) {
        println("No analyzable query units found.")
      } else {
        queryUnits.foreach { q =>
          println(
            s"unitId=${q.unitId}, sourceType=${q.sourceType}, objectName=${q.objectName.getOrElse("N/A")}"
          )
        }
      }

      val analyzedPlans: Seq[(QueryUnit, LogicalPlan)] = queryUnits.map { unit =>
        val analyzed = spark.sql(unit.queryText).queryExecution.analyzed
        (unit, analyzed)
      }

      println("========== analyzed plans ==========")
      analyzedPlans.foreach { case (unit, plan) =>
        println(s"----- ${unit.unitId} / ${unit.objectName.getOrElse("N/A")} -----")
        println(plan.treeString)
      }

      val repeated: Seq[GlobalRepeatedCandidate] =
        SubplanCollector.collectRepeatedAcrossQueries(
          plans = analyzedPlans,
          minOccurrences = minOccurrences,
          minNodeSize = minNodeSize
        )

      println("========== repeated global candidates ==========")
      if (repeated.isEmpty) {
        println("No repeated candidates found.")
      } else {
        repeated.foreach { c =>
          println(
            s"fingerprint=${c.fingerprint}, occurrences=${c.occurrences}, nodeSize=${c.nodeSize}, " +
              s"sources=${c.sourceUnitIds.mkString("[", ", ", "]")}"
          )
        }
      }

      dumpCandidates(repeated, dumpFile)
    } finally {
      spark.stop()
    }
  }

  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.flatMap { arg =>
      arg.split("=", 2) match {
        case Array(k, v) if k.startsWith("--") => Some(k.stripPrefix("--") -> v)
        case _ => None
      }
    }.toMap
  }

  private def readSql(path: String): String = {
    val source = Source.fromFile(path)
    try source.getLines().mkString("\n")
    finally source.close()
  }

  private def dumpCandidates(candidates: Seq[GlobalRepeatedCandidate], path: String): Unit = {
  val writer = new PrintWriter(path)
  try {
    candidates.foreach { c =>
      writer.println("===================================")
      writer.println(s"fingerprint : ${c.fingerprint}")
      writer.println(s"occurrences : ${c.occurrences}")
      writer.println(s"nodeSize    : ${c.nodeSize}")
      writer.println(s"sourceUnits : ${c.sourceUnitIds.mkString(", ")}")
      writer.println(s"sourceNames : ${c.sourceObjectNames.mkString(", ")}")

      writer.println("planText:")
      writer.println(c.examplePlanText)
      writer.println()

      writer.println("sourceSqls:")
      c.sourceSqls.foreach { s =>
        writer.println("-----------------------------------")
        writer.println(s"unitId     : ${s.unitId}")
        writer.println(s"objectName : ${s.objectName.getOrElse("N/A")}")

        writer.println("rawText:")
        writer.println(s.rawText)

        writer.println("queryText:")
        writer.println(s.queryText)
        writer.println()
      }

      writer.println()
    }
  } finally {
    writer.close()
  }
}
}
