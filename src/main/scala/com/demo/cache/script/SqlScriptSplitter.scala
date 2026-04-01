package com.demo.cache.script

object SqlScriptSplitter {

  def split(sqlText: String): Seq[String] = {
    val noComments = removeComments(sqlText)

    noComments
      .split(";")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSeq
  }

  private def removeComments(sqlText: String): String = {
    sqlText
      .linesIterator
      .map { line =>
        val idx = line.indexOf("--")
        if (idx >= 0) line.substring(0, idx) else line
      }
      .mkString("\n")
  }
}
