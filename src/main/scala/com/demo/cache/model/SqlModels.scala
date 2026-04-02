package com.demo.cache.model

case class QueryUnit(
  unitId: String,
  sourceType: String,
  objectName: Option[String],
  rawText: String,
  queryText: String
)

case class CandidateSourceSql(
  unitId: String,
  objectName: Option[String],
  rawText: String,
  queryText: String
)

case class GlobalRepeatedCandidate(
  fingerprint: String,
  occurrences: Int,
  nodeSize: Int,
  sourceUnitIds: Seq[String],
  sourceObjectNames: Seq[String],
  examplePlanText: String,
  sourceSqls: Seq[CandidateSourceSql]   
)
