package com.demo.cache.core

import org.apache.spark.sql.catalyst.plans.logical._

object EligibilityChecker {

  def isCacheable(plan: LogicalPlan): Boolean = {
    plan match {
      case _: Project => true
      case _: Filter => true
      case _: Aggregate => true
      case _: Join => true
      case _: SubqueryAlias => true
      case _: Window => true
      case _: Union => true
      case _: Sort => true
      case _: Distinct => true
      case _ => false
    }
  }
}
