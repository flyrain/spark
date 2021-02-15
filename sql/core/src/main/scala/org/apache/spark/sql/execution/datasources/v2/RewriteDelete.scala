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

import org.apache.iceberg.DistributionMode
import org.apache.iceberg.spark.Spark3Util

import org.apache.spark.sql.catalyst.analysis.IcebergSupport
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{SupportsDelete, Table}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

// copied from Iceberg Spark extensions
object RewriteDelete
  extends Rule[LogicalPlan] with RewriteRowLevelOperationHelper with IcebergSupport {

  import DataSourceV2Implicits._
  import RewriteRowLevelOperationHelper._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // don't rewrite deletes that can be answered using metadata operations
    case d @ DeleteFromTable(r: DataSourceV2Relation, Some(cond))
        if isIcebergRelation(r) && isMetadataDelete(r, cond) =>
      d

    // rewrite all operations that require reading the table to delete records
    case DeleteFromTable(r: DataSourceV2Relation, Some(cond)) if isIcebergRelation(r) =>
      val writeInfo = newWriteInfo(r.schema)
      val mergeBuilder = r.table.asMergeable.newMergeBuilder("delete", writeInfo)

      val matchingRowsPlanBuilder = scanRelation => Filter(cond, scanRelation)
      val scanPlan = buildDynamicFilterScanPlan(r, mergeBuilder, cond, matchingRowsPlanBuilder)

      val remainingRowFilter = Not(EqualNullSafe(cond, Literal(true, BooleanType)))
      val remainingRowsPlan = Filter(remainingRowFilter, scanPlan)

      val mergeWrite = mergeBuilder.asWriteBuilder.build()
      val writePlan = buildWritePlan(remainingRowsPlan, r.table, r.output)
      ReplaceData(r, writePlan, mergeWrite)
  }

  private def buildWritePlan(
      remainingRowsPlan: LogicalPlan,
      table: Table,
      output: Seq[AttributeReference]): LogicalPlan = {

    val fileNameCol = findOutputAttr(remainingRowsPlan.output, FILE_NAME_COL)
    val rowPosCol = findOutputAttr(remainingRowsPlan.output, ROW_POS_COL)

    val icebergTable = Spark3Util.toIcebergTable(table)
    val distributionMode = Spark3Util.distributionModeFor(icebergTable)
    val planWithDistribution = distributionMode match {
      case DistributionMode.NONE =>
        remainingRowsPlan
      case _ =>
        // apply hash partitioning by file if the distribution mode is hash or range
        val numShufflePartitions = SQLConf.get.numShufflePartitions
        RepartitionByExpression(Seq(fileNameCol), remainingRowsPlan, numShufflePartitions)
    }

    val order = Seq(SortOrder(fileNameCol, Ascending), SortOrder(rowPosCol, Ascending))
    val sort = Sort(order, global = false, planWithDistribution)
    Project(output, sort)
  }

  private def isMetadataDelete(relation: DataSourceV2Relation, cond: Expression): Boolean = {
    relation.table match {
      case t: SupportsDelete if !SubqueryExpression.hasSubquery(cond) =>
        val predicates = splitConjunctivePredicates(cond)
        val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, relation.output)
        val dataSourceFilters = toDataSourceFilters(normalizedPredicates)
        val allPredicatesTranslated = normalizedPredicates.size == dataSourceFilters.length
        allPredicatesTranslated && t.canDeleteWhere(dataSourceFilters)
      case _ => false
    }
  }
}
