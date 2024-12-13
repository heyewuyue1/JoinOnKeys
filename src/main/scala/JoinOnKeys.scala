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

  private def replaceExprId(lFilter: Expression, rFilter: Expression): Expression = {
    logInfo("Start to replaceExprId")
    // 收集 lFilter 中所有 AttributeReference 信息
    val lAttributes = lFilter.collect {
      case attr: AttributeReference => attr
    }.map(attr => (attr.name, attr.dataType) -> attr).toMap

    logInfo(s"lAttributes: $lAttributes")

    logInfo(s"Before rFilter: $rFilter")
    // 替换 rFilter 中的 AttributeReference
    val newRFilter = rFilter.transform {
      case attr: AttributeReference =>
        // 如果在 lAttributes 中找到相同名称和类型的 Attribute，则替换为 lFilter 中的引用
        lAttributes.get((attr.name, attr.dataType)).getOrElse(attr)
    }
    logInfo(s"After rFilter: $newRFilter")

    newRFilter
  }

  // 递归修改表达式中的 exprId，使其与 target 的 exprId 保持一致
  private def replaceFilterMapExprId(
                             lFilterMap: Map[LogicalPlan, Expression],
                             rFilterMap: Map[LogicalPlan, Expression]
                           ): Map[LogicalPlan, Expression] = {
    rFilterMap.map { case (rLogicalPlan, rFilter) =>
      logInfo(s"Finding corresponding lFilter for rFilter: $rFilter")
      // 在 lFilterMap 中找到与 rLogicalPlan 等价的 lLogicalPlan
      val lFilterOpt = lFilterMap.collectFirst {
        case (lLogicalPlan, lFilter) if lLogicalPlan.sameResult(rLogicalPlan) =>
          logInfo(s"Collected corresponding lFilter: $lFilter")
          lFilter
      }

      // 如果存在 lFilter，则替换 rFilter 中的 AttributeReference
      val updatedRFilter = lFilterOpt match {
        case Some(lFilter) => replaceExprId(lFilter, rFilter) // 调用之前实现的替换函数
        case None => rFilter // 如果 lFilter 不存在，则保持 rFilter 不变
      }

      // 返回更新后的 (LogicalPlan, Expression) 对
      rLogicalPlan -> updatedRFilter
    }
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
    logInfo(s"fusedFilterMap:${fusedFilterMap}")

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
      rFilterMap = replaceFilterMapExprId(lFilterMap, rFilterMap)
      fusePlan(leftPlan, rightPlan, lFilterMap, rFilterMap)
    } else {
      logInfo("leftPlan unequals rightPlan")
      root
    }
  }
}

