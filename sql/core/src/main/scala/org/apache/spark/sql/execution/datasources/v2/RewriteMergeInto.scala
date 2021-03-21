/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import org.apache.iceberg.TableProperties.MERGE_CARDINALITY_CHECK_ENABLED
import org.apache.iceberg.TableProperties.MERGE_CARDINALITY_CHECK_ENABLED_DEFAULT
import org.apache.iceberg.util.PropertyUtil

import org.apache.spark.sql.catalyst.analysis.IcebergSupport
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, DeleteAction, Filter, InsertAction, Join, JoinHint, LogicalPlan, MergeAction, MergeInto, MergeIntoParams, MergeIntoTable, Project, ReplaceData, UpdateAction}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.write.MergeBuilder
import org.apache.spark.sql.types.BooleanType

// copied from Iceberg Spark extensions
object RewriteMergeInto
  extends Rule[LogicalPlan] with RewriteRowLevelOperationHelper with IcebergSupport {

  import DataSourceV2Implicits._

  private final val ROW_FROM_SOURCE = "_row_from_source_"
  private final val ROW_FROM_TARGET = "_row_from_target_"
  private final val TRUE_LITERAL = Literal(true, BooleanType)
  private final val FALSE_LITERAL = Literal(false, BooleanType)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case MergeIntoTable(r: DataSourceV2Relation, source, cond, matchedActions, notMatchedActions)
          if matchedActions.isEmpty && notMatchedActions.size == 1 && isIcebergRelation(r) =>

        val targetTableScan = buildSimpleScanPlan(r, cond)

        // NOT MATCHED conditions may only refer to columns in source so we can push them down
        val insertAction = notMatchedActions.head.asInstanceOf[InsertAction]
        val filteredSource = insertAction.condition match {
          case Some(insertCond) => Filter(insertCond, source)
          case None => source
        }

        // when there are no matched actions, use a left anti join to remove any matching rows and
        // rewrite to use append instead of replace. only unmatched source rows are passed
        // to the merge and actions are all inserts.
        val joinPlan = Join(filteredSource, targetTableScan, LeftAnti, Some(cond), JoinHint.NONE)

        val outputExprs = insertAction.assignments.map(_.value)
        val outputColNames = r.output.map(_.name)
        val outputCols = outputExprs.zip(outputColNames).map { case (expr, name) =>
          Alias(expr, name)()
        }
        val mergePlan = Project(outputCols, joinPlan)
        val writePlan = buildWritePlan(mergePlan, r.table)
        val writeInfo = newWriteInfo(r.schema)
        val write = r.table.asWritable.newWriteBuilder(writeInfo).build()

        // construct the write here to avoid injecting extra repartition and sort nodes
        AppendData(r, writePlan, Map.empty, isByName = false, Some(write))

      case MergeIntoTable(r: DataSourceV2Relation, source, cond, matchedActions, notMatchedActions)
          if matchedActions.isEmpty && isIcebergRelation(r) =>

        val targetTableScan = buildSimpleScanPlan(r, cond)

        // when there are no matched actions, use a left anti join to remove any matching rows and
        // rewrite to use append instead of replace. only unmatched source rows are passed
        // to the merge and actions are all inserts.
        val joinPlan = Join(source, targetTableScan, LeftAnti, Some(cond), JoinHint.NONE)

        val mergeParams = MergeIntoParams(
          isSourceRowPresent = TRUE_LITERAL,
          isTargetRowPresent = FALSE_LITERAL,
          matchedConditions = Nil,
          matchedOutputs = Nil,
          notMatchedConditions = notMatchedActions.map(getClauseCondition),
          notMatchedOutputs = notMatchedActions.map(actionOutput),
          targetOutput = Nil,
          joinedAttributes = joinPlan.output
        )

        val mergePlan = MergeInto(mergeParams, r.output, joinPlan)
        val writePlan = buildWritePlan(mergePlan, r.table)
        val writeInfo = newWriteInfo(r.schema)
        val write = r.table.asWritable.newWriteBuilder(writeInfo).build()

        // construct the write here to avoid injecting extra repartition and sort nodes
        AppendData(r, writePlan, Map.empty, isByName = false, Some(write))

      case MergeIntoTable(r: DataSourceV2Relation, source, cond, matchedActions, notMatchedActions)
          if notMatchedActions.isEmpty && isIcebergRelation(r) =>

        val mergeBuilder = r.table.asMergeable.newMergeBuilder("merge", newWriteInfo(r.schema))

        // rewrite the matched actions to ensure there is always an action to produce the output row
        val (matchedConditions, matchedOutputs) = rewriteMatchedActions(matchedActions, r.output)

        // when there are no not-matched actions, use a right outer join to ignore source rows
        // that do not match, but keep all unmatched target rows that must be preserved.
        val sourceTableProj = source.output ++ Seq(Alias(TRUE_LITERAL, ROW_FROM_SOURCE)())
        val sourceTableScan = Project(sourceTableProj, source)
        val targetTableScan = buildDynamicFilterTargetScan(
          mergeBuilder, r, source, cond, matchedActions
        )
        val joinPlan = Join(sourceTableScan, targetTableScan, RightOuter, Some(cond), JoinHint.NONE)

        val mergeParams = MergeIntoParams(
          isSourceRowPresent = IsNotNull(findOutputAttr(joinPlan.output, ROW_FROM_SOURCE)),
          isTargetRowPresent = TRUE_LITERAL,
          matchedConditions = matchedConditions,
          matchedOutputs = matchedOutputs,
          notMatchedConditions = Nil,
          notMatchedOutputs = Nil,
          targetOutput = r.output,
          joinedAttributes = joinPlan.output
        )
        val mergePlan = MergeInto(mergeParams, r.output, joinPlan)
        val writePlan = buildWritePlan(mergePlan, r.table)
        val mergeWrite = mergeBuilder.asWriteBuilder.build()

        ReplaceData(r, writePlan, mergeWrite)

      case MergeIntoTable(r: DataSourceV2Relation, source, cond, matchedActions, notMatchedActions)
          if isIcebergRelation(r) =>

        val mergeBuilder = r.table.asMergeable.newMergeBuilder("merge", newWriteInfo(r.schema))

        // rewrite the matched actions to ensure there is always an action to produce the output row
        val (matchedConditions, matchedOutputs) = rewriteMatchedActions(matchedActions, r.output)

        // use a full outer join because there are both matched and not matched actions
        val sourceTableProj = source.output ++ Seq(Alias(TRUE_LITERAL, ROW_FROM_SOURCE)())
        val newSourceTableScan = Project(sourceTableProj, source)
        val targetTableScan = buildDynamicFilterTargetScan(
          mergeBuilder, r, source, cond, matchedActions
        )
        val targetTableProj = targetTableScan.output ++ Seq(Alias(TRUE_LITERAL, ROW_FROM_TARGET)())
        val newTargetTableScan = Project(targetTableProj, targetTableScan)
        val joinPlan = Join(
          newSourceTableScan, newTargetTableScan, FullOuter, Some(cond), JoinHint.NONE
        )

        val mergeParams = MergeIntoParams(
          isSourceRowPresent = IsNotNull(findOutputAttr(joinPlan.output, ROW_FROM_SOURCE)),
          isTargetRowPresent = IsNotNull(findOutputAttr(joinPlan.output, ROW_FROM_TARGET)),
          matchedConditions = matchedConditions,
          matchedOutputs = matchedOutputs,
          notMatchedConditions = notMatchedActions.map(getClauseCondition),
          notMatchedOutputs = notMatchedActions.map(actionOutput),
          targetOutput = r.output,
          joinedAttributes = joinPlan.output
        )
        val mergePlan = MergeInto(mergeParams, r.output, joinPlan)
        val writePlan = buildWritePlan(mergePlan, r.table)
        val mergeWrite = mergeBuilder.asWriteBuilder.build()

        ReplaceData(r, writePlan, mergeWrite)
    }
  }

  private def actionOutput(clause: MergeAction): Option[Seq[Expression]] = {
    clause match {
      case u: UpdateAction =>
        Some(u.assignments.map(_.value))
      case _: DeleteAction =>
        None
      case i: InsertAction =>
        Some(i.assignments.map(_.value))
    }
  }

  private def getClauseCondition(clause: MergeAction): Expression = {
    clause.condition.getOrElse(TRUE_LITERAL)
  }

  private def buildDynamicFilterTargetScan(
      mergeBuilder: MergeBuilder,
      target: DataSourceV2Relation,
      source: LogicalPlan,
      cond: Expression,
      matchedActions: Seq[MergeAction]): LogicalPlan = {
    // Construct the plan to prune target based on join condition between source and target.
    val table = target.table
    val matchingRowsPlanBuilder = rel => Join(source, rel, Inner, Some(cond), JoinHint.NONE)
    val withCheck = isCardinalityCheckEnabled(table) && isCardinalityCheckNeeded(matchedActions)
    buildDynamicFilterScanPlan(target, mergeBuilder, cond, matchingRowsPlanBuilder, withCheck)
  }

  private def rewriteMatchedActions(
      matchedActions: Seq[MergeAction],
      targetOutput: Seq[Expression]): (Seq[Expression], Seq[Option[Seq[Expression]]]) = {
    val startMatchedConditions = matchedActions.map(getClauseCondition)
    val catchAllIndex = startMatchedConditions.indexWhere {
      case Literal(true, BooleanType) =>
        true
      case _ =>
        false
    }

    val outputs = matchedActions.map(actionOutput)
    if (catchAllIndex < 0) {
      // all of the actions have non-trivial conditions
      // add an action to emit the target row if no action matches
      (startMatchedConditions :+ TRUE_LITERAL, outputs :+ Some(targetOutput))
    } else {
      // one "catch all" action will always match, prune the actions after it
      (startMatchedConditions.take(catchAllIndex + 1), outputs.take(catchAllIndex + 1))
    }
  }

  private def isCardinalityCheckEnabled(table: Table): Boolean = {
    PropertyUtil.propertyAsBoolean(
      table.properties(),
      MERGE_CARDINALITY_CHECK_ENABLED,
      MERGE_CARDINALITY_CHECK_ENABLED_DEFAULT)
  }

  private def isCardinalityCheckNeeded(actions: Seq[MergeAction]): Boolean = {
    def hasUnconditionalDelete(action: Option[MergeAction]): Boolean = {
      action match {
        case Some(DeleteAction(None)) => true
        case _ => false
      }
    }
    !(actions.size == 1 && hasUnconditionalDelete(actions.headOption))
  }
}
