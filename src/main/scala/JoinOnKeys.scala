package cn.edu.ruc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, UnknownRuleId}
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, JOIN}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions.{Expression, Or}

object JoinOnKeys extends Rule[LogicalPlan] with Logging{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDownWithPruning(_.containsPattern(JOIN), UnknownRuleId) {
      case j @ Join(l: LogicalPlan, r: LogicalPlan, _, _, _) =>
        logInfo("Join detected")
        logInfo(s"Left plan: ${l.toString()}")
        logInfo(s"Right plan: ${l.toString()}")
        fuseIfCan(j)
    }
  }

  // 从逻辑计划中提取出所有的过滤条件
  private def eliminateFilter(p: LogicalPlan): Map[LogicalPlan, Expression] = {
    var filterMap: Map[LogicalPlan, Expression]= Map()
    p.transformUpWithPruning(_.containsPattern(FILTER), UnknownRuleId) {
      case f @ Filter(condition, child) =>
        filterMap += (child -> condition)
        child
    }
    filterMap
  }


  // 合并两个在同一个子计划上进行的过滤条件
  private def fuseFilter(lFilterMap: Map[LogicalPlan, Expression], rFilterMap: Map[LogicalPlan, Expression]): Map[LogicalPlan, Expression] = {
    var filterMap: Map[LogicalPlan, Expression] = Map()
    lFilterMap.foreach { case (l, lFilter) =>
      rFilterMap.foreach { case (r, rFilter) =>
        if (l.sameResult(r)) {
          // lFilter和rFilter的条件是OR关系
          filterMap += (l -> Or(lFilter, rFilter))
        }
      }
    }
    logInfo(s"Fused filerMap: $filterMap")
    filterMap
  }

  private def fusePlan(leftPlan: LogicalPlan, rightPlan: LogicalPlan,
                       lFilterMap: Map[LogicalPlan, Expression], rFilterMap: Map[LogicalPlan, Expression]
                      ): LogicalPlan = {
    val fusedFilterMap = fuseFilter(lFilterMap, rFilterMap)
    val newPlan = leftPlan.transformUpWithPruning(_.containsPattern(FILTER), UnknownRuleId) {
      case f @ Filter(_, _) =>
        fusedFilterMap.get(f) match {
          case Some(newCondition) => Filter(newCondition, f.child)
          case None => f
        }
    }
    logInfo(s"Current newPlan: $newPlan")
    newPlan
  }

  /*
  :- Aggregate [count(1) AS h8_30_to_9#1L]
:  +- Project
:     +- Join Inner, (ss_store_sk#11L = s_store_sk#43L)
:        :- Project [ss_store_sk#11L]
:        :  +- Join Inner, (ss_sold_time_sk#5L = t_time_sk#33L)
:        :     :- Project [ss_sold_time_sk#5L, ss_store_sk#11L]
:        :     :  +- Join Inner, (ss_hdemo_sk#9L = hd_demo_sk#28L)
:        :     :     :- Project [ss_sold_time_sk#5L, ss_hdemo_sk#9L, ss_store_sk#11L]
:        :     :     :  +- Relation spark_catalog.tpcds_2g.store_sales[ss_sold_time_sk#5L,ss_item_sk#6L,ss_customer_sk#7L,ss_cdemo_sk#8L,ss_hdemo_sk#9L,ss_addr_sk#10L,ss_store_sk#11L,ss_promo_sk#12L,ss_ticket_number#13L,ss_quantity#14,ss_wholesale_cost#15,ss_list_price#16,ss_sales_price#17,ss_ext_discount_amt#18,ss_ext_sales_price#19,ss_ext_wholesale_cost#20,ss_ext_list_price#21,ss_ext_tax#22,ss_coupon_amt#23,ss_net_paid#24,ss_net_paid_inc_tax#25,ss_net_profit#26,ss_sold_date_sk#27L] parquet
:        :     :     +- Project [hd_demo_sk#28L]
:        :     :        +- Filter ((((hd_dep_count#31L = 3) AND (hd_vehicle_count#32L <= 5)) OR ((hd_dep_count#31L = 0) AND (hd_vehicle_count#32L <= 2))) OR ((hd_dep_count#31L = 1) AND (hd_vehicle_count#32L <= 3)))
:        :     :           +- Relation spark_catalog.tpcds_2g.household_demographics[hd_demo_sk#28L,hd_income_band_sk#29L,hd_buy_potential#30,hd_dep_count#31L,hd_vehicle_count#32L] parquet
:        :     +- Project [t_time_sk#33L]
:        :        +- Filter ((t_hour#36L = 8) AND (t_minute#37L >= 30))
:        :           +- Relation spark_catalog.tpcds_2g.time_dim[t_time_sk#33L,t_time_id#34,t_time#35L,t_hour#36L,t_minute#37L,t_second#38L,t_am_pm#39,t_shift#40,t_sub_shift#41,t_meal_time#42] parquet
:        +- Project [s_store_sk#43L]
:           +- Filter (s_store_name#48 = ese)
:              +- Relation spark_catalog.tpcds_2g.store[s_store_sk#43L,s_store_id#44,s_rec_start_date#45,s_rec_end_date#46,s_closed_date_sk#47L,s_store_name#48,s_number_employees#49,s_floor_space#50,s_hours#51,s_manager#52,s_market_id#53,s_geography_class#54,s_market_desc#55,s_market_manager#56,s_division_id#57,s_division_name#58,s_company_id#59,s_company_name#60,s_street_number#61,s_street_name#62,s_street_type#63,s_suite_number#64,s_city#65,s_county#66,... 5 more fields] parquet
   */
  /*
  * +- Aggregate [count(1) AS h9_to_9_30#2L]
   +- Project
      +- Join Inner, (ss_store_sk#78L = s_store_sk#110L)
         :- Project [ss_store_sk#78L]
         :  +- Join Inner, (ss_sold_time_sk#72L = t_time_sk#100L)
         :     :- Project [ss_sold_time_sk#72L, ss_store_sk#78L]
         :     :  +- Join Inner, (ss_hdemo_sk#76L = hd_demo_sk#95L)
         :     :     :- Project [ss_sold_time_sk#72L, ss_hdemo_sk#76L, ss_store_sk#78L]
         :     :     :  +- Relation spark_catalog.tpcds_2g.store_sales[ss_sold_time_sk#72L,ss_item_sk#73L,ss_customer_sk#74L,ss_cdemo_sk#75L,ss_hdemo_sk#76L,ss_addr_sk#77L,ss_store_sk#78L,ss_promo_sk#79L,ss_ticket_number#80L,ss_quantity#81,ss_wholesale_cost#82,ss_list_price#83,ss_sales_price#84,ss_ext_discount_amt#85,ss_ext_sales_price#86,ss_ext_wholesale_cost#87,ss_ext_list_price#88,ss_ext_tax#89,ss_coupon_amt#90,ss_net_paid#91,ss_net_paid_inc_tax#92,ss_net_profit#93,ss_sold_date_sk#94L] parquet
         :     :     +- Project [hd_demo_sk#95L]
         :     :        +- Filter ((((hd_dep_count#98L = 3) AND (hd_vehicle_count#99L <= 5)) OR ((hd_dep_count#98L = 0) AND (hd_vehicle_count#99L <= 2))) OR ((hd_dep_count#98L = 1) AND (hd_vehicle_count#99L <= 3)))
         :     :           +- Relation spark_catalog.tpcds_2g.household_demographics[hd_demo_sk#95L,hd_income_band_sk#96L,hd_buy_potential#97,hd_dep_count#98L,hd_vehicle_count#99L] parquet
         :     +- Project [t_time_sk#100L]
         :        +- Filter ((t_hour#103L = 9) AND (t_minute#104L < 30))
         :           +- Relation spark_catalog.tpcds_2g.time_dim[t_time_sk#100L,t_time_id#101,t_time#102L,t_hour#103L,t_minute#104L,t_second#105L,t_am_pm#106,t_shift#107,t_sub_shift#108,t_meal_time#109] parquet
         +- Project [s_store_sk#110L]
            +- Filter (s_store_name#115 = ese)
               +- Relation spark_catalog.tpcds_2g.store[s_store_sk#110L,s_store_id#111,s_rec_start_date#112,s_rec_end_date#113,s_closed_date_sk#114L,s_store_name#115,s_number_employees#116,s_floor_space#117,s_hours#118,s_manager#119,s_market_id#120,s_geography_class#121,s_market_desc#122,s_market_manager#123,s_division_id#124,s_division_name#125,s_company_id#126,s_company_name#127,s_street_number#128,s_street_name#129,s_street_type#130,s_suite_number#131,s_city#132,s_county#133,... 5 more fields] parquet
  *
  * */
  private def fuseIfCan(root: LogicalPlan): LogicalPlan = {
    val Join(leftPlan, rightPlan, _, _, _) = root
    // 记录l和r涉及的每个表的过滤条件,leftPlan如果是引用的话会有问题
    val lFilterMap: Map[LogicalPlan, Expression] = eliminateFilter(leftPlan)
    val rFilterMap: Map[LogicalPlan, Expression] = eliminateFilter(rightPlan)
    if (leftPlan.sameResult(rightPlan)) {
      logInfo("leftPlan equals rightPlan (without filter)")
      fusePlan(leftPlan, rightPlan, lFilterMap, rFilterMap)
    } else {
      logInfo("leftPlan unequals rightPlan")
      root
    }
  }

}

