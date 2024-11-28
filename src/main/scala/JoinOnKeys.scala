package cn.edu.ruc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object JoinOnKeys extends Rule[LogicalPlan] with Logging{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    logInfo(plan.verboseStringWithSuffix(10000))
    plan
  }
}
