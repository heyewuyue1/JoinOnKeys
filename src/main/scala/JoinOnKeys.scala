package cn.edu.ruc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, UnknownRuleId}
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, JOIN, PROJECT}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, ConstantFolding, NullPropagation, SimplifyConditionals}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

object JoinOnKeys extends Rule[LogicalPlan] with Logging{

  // 合成布尔索引列的ID，因为新合成的列需要有一个名字
  private var curNamedExpressionID: Int = 0
  private var PlanTemplate:Option[LogicalPlan] = None
  private var FilterMap:mutable.Map[Int,(NamedExpression, Char)] = mutable.Map()

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
        logInfo(s"Join detected with type: ${j.joinType}")
          fuseIfCan(j)
    }
    newPlan = simplifyLogicalPlan(newPlan)
    logInfo(s"newPlan: ${newPlan.toString()}")
    logInfo(s"lenFilterMap:${FilterMap.size}")
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
    //logInfo("Start to replaceExprId")
    // 收集 lFilter 中所有 AttributeReference 信息
    val lAttributes = lFilter.collect {
      case attr: AttributeReference => attr
    }.map(attr => (attr.name, attr.dataType) -> attr).toMap

    //logInfo(s"lAttributes: $lAttributes")

    //logInfo(s"Before rFilter: $rFilter")
    // 替换 rFilter 中的 AttributeReference
    val newRFilter = rFilter.transform {
      case attr: AttributeReference =>
        // 如果在 lAttributes 中找到相同名称和类型的 Attribute，则替换为 lFilter 中的引用
        lAttributes.get((attr.name, attr.dataType)).getOrElse(attr)
    }
    //logInfo(s"After rFilter: $newRFilter")

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
    filterMap
  }

  // 合成布尔索引列
  private def makeBoolIndex(filterMap: Map[LogicalPlan, Expression],LorR: Char) :(NamedExpression,ExprId, Set[AttributeReference])  = {
    var columnsSet: Set[AttributeReference] = Set()
    //null的真值是false，但实际上什么都没有应该是true
    var boolIndexExp: Expression = Literal(null, StringType)  //  = null
    filterMap.foreach{case (subPlan: LogicalPlan, filter: Expression) =>
      if (boolIndexExp.semanticEquals(Literal(null, StringType))) {
        boolIndexExp = filter
      } else {
        boolIndexExp = And(filter, boolIndexExp)
      }
      columnsSet ++= extractColumns(filter)
    }
//    logInfo(s"NamedExpression$curNamedExpressionID: $boolIndexExp")
    val namedBoolIndexExp: NamedExpression = Alias(boolIndexExp, s"NamedExpression$curNamedExpressionID")()
    FilterMap += (curNamedExpressionID -> (namedBoolIndexExp,LorR))
    curNamedExpressionID += 1
    (namedBoolIndexExp,namedBoolIndexExp.exprId, columnsSet)
  }

  private def insertFilterIntoAggregate(agg: Seq[NamedExpression], filter: NamedExpression, exprid: ExprId): Seq[NamedExpression] = {
    agg.map {
      case Alias(aggExpr: AggregateExpression, name) =>
        Alias(
          aggExpr.copy(filter = Some(AttributeReference(filter.name, filter.dataType)(exprId = exprid))), // 更新 AggregateExpression 的 filter，保留exprid
          name
        )()
      case other =>
        other // 保留其他 NamedExpression 不变
    }
  }

  private def insertFilterIntoAggregateLL(len:Int, aggs: Seq[NamedExpression], filters: Seq[NamedExpression]): Seq[NamedExpression] = {
    val filterIter = filters.iterator // 创建迭代器
    aggs.grouped(len).flatMap { aggGroup =>
      val filter = filterIter.next() // 取下一个 filter，不足时用最后一个 filter
      aggGroup.map {
        case Alias(aggExpr: AggregateExpression, name) =>
          Alias(
            aggExpr.copy(filter = Some(AttributeReference(filter.name, filter.dataType)(exprId = filter.exprId))),
            name
          )()
        case otherAgg => otherAgg // 非 AggregateExpression 直接返回
      }
    }.toSeq
  }

  private def extractColumns(expression: Expression): Set[AttributeReference] = {
    expression match {
      case attr: AttributeReference => Set(attr) // If it's an AttributeReference, return it as a set
      case andExpr: And =>
        extractColumns(andExpr.left) ++ extractColumns(andExpr.right)
      case orExpr: Or =>
        extractColumns(orExpr.left) ++ extractColumns(orExpr.right)
      case equalExpr: EqualTo =>
        extractColumns(equalExpr.left) ++ extractColumns(equalExpr.right)
      case _ =>
        // For any other expression types, recursively extract from children (if they exist)
        expression.children.flatMap(extractColumns).toSet
    }
  }

  private def extractColumnsFromAggExpressions(aggs: Seq[NamedExpression]): Seq[AttributeReference] = {
    aggs.flatMap {
      case Alias(child, _) => extractColumns2(child) // 解析 Alias
      case other => extractColumns2(other) // 直接解析
    }.distinct // 去重
  }

  private def extractColumns2(expr: Expression): Seq[AttributeReference] = {
    expr match {
      case agg: AggregateExpression => extractColumns2(agg.aggregateFunction) // 解析 AggregateExpression
      case func: AggregateFunction => func.children.collect { case attr: AttributeReference => attr } // 提取 AttributeReference
      case _ => Seq.empty
    }
  }
  private def extractAliasesFromAggExpressions(aggs: Seq[NamedExpression]): Seq[String] = {
    aggs.collect {
      case Alias(_, aliasName) => aliasName // 提取 Alias 中的别名
    }
  }

  private def replaceAliases(lAggExpr: Seq[NamedExpression], ralias: Seq[String]): Seq[NamedExpression] = {
    lAggExpr.zip(ralias).map {
      case (Alias(aggExpr, name),newalias) =>
        Alias(aggExpr,newalias)()// 返回 NamedExpression
    }
  }

  private def replaceAttrRefInAggExpressions(attrs: Seq[AttributeReference], rAggExprs: Seq[NamedExpression]): Seq[NamedExpression] = {
    val lAttributes = attrs.map(attr => attr.name -> attr).toMap
    // 替换 rAggExpr 中的 AttributeReference
    rAggExprs.map{
      case Alias(rAggExpr, aliasName) =>
        Alias(
        rAggExpr.transform {
          case attr: AttributeReference =>
            // 如果在 lAttributes 中找到相同名称和类型的 Attribute，则替换为 lFilter 中的引用
            lAttributes.get(attr.name).getOrElse(attr)
        }, aliasName)()
    }
  }

  // 融合leftPlan和rightPlan
  private def fusePlan(leftPlan: LogicalPlan, rightPlan: LogicalPlan,
                       lFilterMap: Map[LogicalPlan, Expression], rFilterMap: Map[LogicalPlan, Expression],
                       isM: Boolean
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

    // 构建布尔索引 并记录其ExprID、其中filter涉及的列的集合
    var Aggregate(lGroupExpr, lAggExpr, _) = leftPlan
    var Aggregate(_, rAggExpr, _) = rightPlan
    val (leftBoolIndex, leftBoolIndex_ExprID, left_cols) =
      if (isM) makeBoolIndex(lFilterMap, 'l') else makeBoolIndex(lFilterMap, 'r') // 如果是第一次JOK，则把left和right都记为r。
    val NExpressions: Seq[NamedExpression] = FilterMap.toSeq  // left部分所有的NamedExpression
      .sortBy(_._1)    // 按 key (Int) 递增排序
      .collect { case (_, (expr, 'r')) => expr }  // 筛选 Char == 'r'，提取 NamedExpression
      .takeRight(lAggExpr.length/rAggExpr.length)  // 取最后(lAggExpr.length/rAggExpr.length)个
    val (rightBoolIndex,rightBoolIndex_ExprID, right_cols) = makeBoolIndex(rFilterMap,'r')  //标记为r的是每个子查询对应的NamedExpression
    FilterMap.foreach { case (key, value) =>
      logInfo(s"FilterMap  Key: $key, Value: $value")
    }
    val combinedSet = left_cols ++ right_cols   // combinedSet即必须传递到最上层project的所有列

    //为每个project的output添加后续需要的列
    val columns = extractColumnsFromAggExpressions(lAggExpr)  // 提取aggregate中agg内涉及到的所有列
    // if (columns.nonEmpty) {val aggExprId = columns.head.exprId}
    //val ralias = extractAliasesFromAggExpressions(rAggExpr)
    val rAggAfter = replaceAttrRefInAggExpressions(columns, rAggExpr)  // 令right部分agg内的列ExprId与left的一致（用left的agg，替换为right的别名）
    //logInfo(s"AggCols:$columns, rAlias:$ralias, rAggAfter:$rAggAfter")
    logInfo(s"AggCols:$columns, rAggAfter:$rAggAfter")
    newPlan = newPlan.transformUpWithPruning(_.containsPattern(PROJECT), UnknownRuleId) {
      case project @ Project(projectList, child) =>
        val inputcols = child.output
        val outputcols = project.output
        val inputColsAsRef = inputcols.collect { case attr: AttributeReference => attr }.toSet  // 类型转换
        val outputColsAsRef = outputcols.collect { case attr: AttributeReference => attr }.toSet
        //logInfo(s"inputColsAsRef:$inputColsAsRef, outputColsAsRef:$outputColsAsRef")
        val missingColumns = inputColsAsRef.diff(outputColsAsRef) // IN  inputCols,  NOTIN outputColumns
        //logInfo(s"missingColumns:$missingColumns")
        val intersectingColumns = missingColumns.filter(missingAttr => // 找交集，比较内容而非引用
          combinedSet.exists(combinedAttr =>
            missingAttr.name == combinedAttr.name && missingAttr.dataType == combinedAttr.dataType
          )
        )
        //logInfo(s"intersectingColumns:$intersectingColumns")
        if (intersectingColumns.nonEmpty) {
          val newProjectList = projectList ++ intersectingColumns ++ columns  // ovo
          val newProject = project.copy(projectList = newProjectList)
          newProject
        } else {
          project
        }
        }
    // 在newPlan上Project出columns、left涉及的NamedExpression、right部分的NamedExpression
    newPlan = Project(columns++NExpressions:+rightBoolIndex, newPlan.children.head.children.head)

    // agg中的 filter(where NamedExpression0#ExprId) 中的ExprId应该和projet中相应的值相同
    logInfo(s"lGroupExpr:$lGroupExpr")
    logInfo(s"lAggExprBefore:$lAggExpr")
    logInfo(s"NExpressions:$NExpressions")
    lAggExpr = insertFilterIntoAggregateLL(rAggExpr.length,lAggExpr, NExpressions)
    logInfo(s"lAggExprAfter:$lAggExpr")
    rAggExpr = insertFilterIntoAggregate(rAggAfter, rightBoolIndex, rightBoolIndex_ExprID)  //

    logInfo(s"length: left=${lAggExpr.length}, right=${rAggExpr.length}")
    logInfo(s"Agg: left=${lAggExpr}, right=${rAggExpr}")
    newPlan = Aggregate(lGroupExpr, lAggExpr ++ rAggExpr, newPlan)
    newPlan
  }

  // 如果可以融合则融合root的左右节点
  private def fuseIfCan(root: LogicalPlan): LogicalPlan = {
    val Join(leftPlan, rightPlan, _, _, _) = root

    // 记录l和r涉及的每个表的过滤条件
    var (leftP_After, lFilterMap) = eliminateFilter(leftPlan)
    var (rightP_After, rFilterMap) = eliminateFilter(rightPlan)
    var isM = false
    if (leftP_After.sameResult(rightP_After)) {
      logInfo("leftPlan equals rightPlan (without filter)")
      logInfo(s"leftPlan:$leftPlan")
      // 对rFilterMap中的每个过滤条件，将其替换为与lFilterMap中对应的过滤条件exprID相等
      rFilterMap = replaceFilterMapExprId(lFilterMap, rFilterMap)

      if (PlanTemplate.isEmpty)
        PlanTemplate = Some(leftP_After)

      fusePlan(leftPlan, rightPlan, lFilterMap, rFilterMap, isM)
    } else {
      logInfo("leftPlan unequals rightPlan")

      if (PlanTemplate.isDefined && rightP_After.sameResult(PlanTemplate.get) && leftPlan.isInstanceOf[Aggregate]) {
        isM = true
        logInfo("rightPlan equals Template (without filter)")
        logInfo(s"leftPlan:${leftPlan}, rightPlan:${rightPlan}")
        rFilterMap = replaceFilterMapExprId(lFilterMap, rFilterMap)
        val new2Plan = fusePlan(leftPlan, rightPlan, lFilterMap, rFilterMap, isM)
        logInfo(s"new2Plan:${new2Plan}")
        return new2Plan
      }
      root
    }
  }
}

