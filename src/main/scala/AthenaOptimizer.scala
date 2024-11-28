package cn.edu.ruc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

class AthenaOptimizer extends Function1[SparkSessionExtensions, Unit] with Logging{
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(session => JoinOnKeys)
  }
}
