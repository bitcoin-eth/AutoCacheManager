package com.demo.cache.core

import java.security.MessageDigest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object PlanFingerprint {

  def of(plan: LogicalPlan): String = {
    val normalized = normalize(plan.treeString)
    md5(normalized)
  }

  private def normalize(text: String): String = {
    text
      .replaceAll("#\\d+", "#x")
      .replaceAll("`", "")
      .replaceAll("\\s+", " ")
      .trim
      .toLowerCase
  }

  private def md5(s: String): String = {
    val md = MessageDigest.getInstance("MD5")
    md.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
}
