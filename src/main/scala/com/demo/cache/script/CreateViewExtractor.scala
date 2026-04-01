package com.demo.cache.script

import com.demo.cache.model.QueryUnit

object CreateViewExtractor {

  private val CreateViewPattern =
    ("(?is)^create\\s+(?:or\\s+replace\\s+)?" +
      "(?:temporary\\s+|temp\\s+)?" +
      "view\\s+([a-zA-Z0-9_\\.]+)\\s+as\\s+(select.+)$").r

  private val WithOrSelectPattern =
    "(?is)^(with\\s+.+|select\\s+.+)$".r

  def extract(statements: Seq[String]): Seq[QueryUnit] = {
    statements.zipWithIndex.flatMap { case (stmt, idx) =>
      val cleaned = stmt.trim

      cleaned match {
        case CreateViewPattern(viewName, queryText) =>
          Some(
            QueryUnit(
              unitId = s"unit_${idx + 1}",
              sourceType = "CREATE_VIEW",
              objectName = Some(viewName),
              rawText = cleaned,
              queryText = queryText.trim
            )
          )

        case WithOrSelectPattern(_) =>
          Some(
            QueryUnit(
              unitId = s"unit_${idx + 1}",
              sourceType = "FINAL_QUERY",
              objectName = None,
              rawText = cleaned,
              queryText = cleaned
            )
          )

        case _ => None
      }
    }
  }
}
