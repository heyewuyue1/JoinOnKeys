package cn.edu.ruc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, UnknownRuleId}
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, JOIN}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.{SimplifyConditionals, BooleanSimplification, ConstantFolding, NullPropagation}
import org.apache.spark.sql.types.StringType

object JoinOnKeys extends Rule[LogicalPlan] with Logging{

  // 合成布尔索引列的ID，因为新合成的列需要有一个名字
  private var curNamedExpressionID: Int = 0
//  private var indexPlan: Seq[LogicalPlan] = Seq()

  // 辅助方法：对 LogicalPlan 进行表达式简化
  private def simplifyLogicalPlan(plan: LogicalPlan): LogicalPlan = {
    val rules: Seq[Rule[LogicalPlan]] = Seq(
      SimplifyConditionals,
      BooleanSimplification,
      ConstantFolding,
      NullPropagation
    )
    rules.foldLeft(plan) { (currentPlan, rule) =>
      rule(currentPlan)
    }
  }

  // 对所有Join节点的左右子节点判断fuseIfCan
  override def apply(plan: LogicalPlan): LogicalPlan = {
    var newPlan = plan.transformDownWithPruning(_.containsPattern(JOIN), UnknownRuleId) {
      case j @ Join(l: LogicalPlan, r: LogicalPlan, _, _, _) =>
        logInfo("Join detected")
        fuseIfCan(j)
    }
    newPlan = simplifyLogicalPlan(newPlan)
    logInfo(s"newPlan: ${newPlan.toString()}")
    newPlan
  }

  // 从逻辑计划中提取出所有的过滤条件
  private def eliminateFilter(p: LogicalPlan):  (LogicalPlan, Map[LogicalPlan, Expression]) = {
    var filterMap: Map[LogicalPlan, Expression]= Map()
    val newPlan = p.transformUpWithPruning(_.containsPattern(FILTER), UnknownRuleId) {
      case f @ Filter(condition, child) =>
        filterMap += (child -> condition)
        child
    }
    (newPlan, filterMap)
  }

  // 递归修改表达式中的 exprId，使其与 target 的 exprId 保持一致
  private def replaceExprId(rFilter: Expression, lFilter: Expression): Expression = {
    // 找到 rFilter 中的每个叶子节点，并将 exprId 替换为 lFilter 的 exprId
    rFilter.transform {
      case attr: AttributeReference =>
        // 复制 attr，并使用 lFilter 中对应的 exprId
        val newExprId = lFilter.collectFirst {
          case a: AttributeReference if a.semanticEquals(attr) => a.exprId
        }.getOrElse(attr.exprId)
        attr.withExprId(newExprId)
    }
  }

  // 合并两个在同一个子计划上进行的过滤条件
  private def fuseFilter(lFilterMap: Map[LogicalPlan, Expression], rFilterMap: Map[LogicalPlan, Expression]): Map[LogicalPlan, Expression] = {
    var filterMap: Map[LogicalPlan, Expression] = Map()
    lFilterMap.foreach { case (l, lFilter) =>
      rFilterMap.foreach { case (r, rFilter) =>
        if (l.sameResult(r)) {
          // lFilter和rFilter的条件是OR关系
          filterMap += (l -> Or(lFilter, replaceExprId(lFilter,rFilter)))
        }
      }
    }
//    logInfo(s"Fused filerMap: $filterMap")
    filterMap
  }

  // 合成布尔索引列
  private def makeBoolIndex(filterMap: Map[LogicalPlan, Expression]) :NamedExpression = {

    //null的真值是false，但实际上什么都没有应该是true
    var boolIndexExp: Expression = Literal(null, StringType)  //  = null
    filterMap.foreach{case (subPlan: LogicalPlan, filter: Expression) =>
      if (boolIndexExp.semanticEquals(Literal(null, StringType))) {
        boolIndexExp = filter
      } else {
        boolIndexExp = And(filter, boolIndexExp)
      }
    }
//    logInfo(s"NamedExpression$curNamedExpressionID: $boolIndexExp")
    val namedBoolIndexExp: NamedExpression = Alias(boolIndexExp, s"NamedExpression$curNamedExpressionID")()
    curNamedExpressionID += 1
    namedBoolIndexExp
  }

  private def insertFilterIntoAggregate(agg: Seq[NamedExpression], filter: NamedExpression): Seq[NamedExpression] = {
    agg.map {
      case Alias(aggExpr: AggregateExpression, name) =>
        Alias(
          aggExpr.copy(filter = Some(AttributeReference(filter.name, filter.dataType)())), // 更新 AggregateExpression 的 filter
          name
        )()
      case other =>
        other // 保留其他 NamedExpression 不变
    }
  }

  // 融合leftPlan和rightPlan
  private def fusePlan(leftPlan: LogicalPlan, rightPlan: LogicalPlan,
                       lFilterMap: Map[LogicalPlan, Expression], rFilterMap: Map[LogicalPlan, Expression]
                      ): LogicalPlan = {

    val fusedFilterMap = fuseFilter(lFilterMap, rFilterMap)
    logInfo(s"fusedFMap:${fusedFilterMap}")

    var newPlan = leftPlan.transformDownWithPruning(_.containsPattern(FILTER), UnknownRuleId) {
      case f @ Filter(_, _) =>
        fusedFilterMap.get(f.child) match {
          case Some(newCondition) => Filter(newCondition, f.child)
          case None => f
        }
    }

    // 构建布尔索引
    val leftBoolIndex = makeBoolIndex(lFilterMap)
    val rightBoolIndex = makeBoolIndex(rFilterMap)
    // 在newPlan上Project出leftPlan和rightPlan的所有列以及布尔索引，output有问题
    newPlan = Project(Seq(leftBoolIndex, rightBoolIndex), newPlan.children.head.children.head)

    var Aggregate(lGroupExpr, lAggExpr, _) = leftPlan
    var Aggregate(_, rAggExpr, _) = rightPlan

    lAggExpr = insertFilterIntoAggregate(lAggExpr, leftBoolIndex)
    rAggExpr = insertFilterIntoAggregate(rAggExpr, rightBoolIndex)

    newPlan = Aggregate(lGroupExpr, lAggExpr ++ rAggExpr, newPlan)

    newPlan
  }

  // 如果可以融合则融合root的左右节点
  private def fuseIfCan(root: LogicalPlan): LogicalPlan = {
    val Join(leftPlan, rightPlan, _, _, _) = root

    // 记录l和r涉及的每个表的过滤条件
    var (leftP_After, lFilterMap) = eliminateFilter(leftPlan)
    var (rightP_After, rFilterMap) = eliminateFilter(rightPlan)

    if (leftP_After.sameResult(rightP_After)) {
      logInfo("leftPlan equals rightPlan (without filter)")

      // 对rFilterMap中的每个过滤条件，将其替换为与lFilterMap中对应的过滤条件exprID相等
      rFilterMap.foreach{ case (r, rFilter) =>
        lFilterMap.foreach({ case (l, lFilter) =>
          if (r.sameResult(l)) {
            rFilterMap += (r -> replaceExprId(rFilter, lFilter))
          }
        })
      }
      fusePlan(leftPlan, rightPlan, lFilterMap, rFilterMap)
    } else {
      logInfo("leftPlan unequals rightPlan")
      root
    }
  }
}

