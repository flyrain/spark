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
import org.apache.iceberg.spark.{Spark3Util, SparkSchemaUtil}

import org.apache.spark.sql.catalyst.analysis.IcebergSupport
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.{SortOrder, Transform}
import org.apache.spark.sql.types.StructType

object V2IcebergWrites extends Rule[LogicalPlan] with IcebergSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case ctas @ CreateTableAsSelect(catalog, _, transforms, query, properties, _, _,
        distributionMode, ordering) if isIcebergTable(catalog, properties) =>

      val schema = CharVarcharUtils.getRawSchema(query.schema).asNullable

      val (requiredDistribution, requiredOrdering) =
        toRequiredDistributionAndOrdering(schema, transforms, distributionMode, ordering)

      val newQuery = DistributionAndOrderingUtils.prepareQuery(
        requiredDistribution,
        requiredOrdering,
        query,
        conf)
      ctas.copy(query = newQuery)

    case rtas @ ReplaceTableAsSelect(catalog, _, transforms, query, properties, _, _,
        distributionMode, ordering) if isIcebergTable(catalog, properties) =>

      val schema = CharVarcharUtils.getRawSchema(query.schema).asNullable

      val (requiredDistribution, requiredOrdering) =
        toRequiredDistributionAndOrdering(schema, transforms, distributionMode, ordering)

      val newQuery = DistributionAndOrderingUtils.prepareQuery(
        requiredDistribution,
        requiredOrdering,
        query,
        conf)
      rtas.copy(query = newQuery)
  }

  private def toRequiredDistributionAndOrdering(
      schema: StructType,
      transforms: Seq[Transform],
      distributionMode: String,
      ordering: Seq[SortOrder]): (Distribution, Array[SortOrder]) = {

    val icebergSchema = SparkSchemaUtil.convert(schema)
    val icebergPartitionSpec = Spark3Util.toPartitionSpec(icebergSchema, transforms.toArray)
    val icebergDistributionMode = DistributionMode.fromName(distributionMode)
    val icebergSortOrder = Spark3Util.toSortOrder(icebergSchema, ordering.toArray)

    val requiredDistribution = Spark3Util.buildRequiredDistribution(
      icebergDistributionMode,
      icebergSchema,
      icebergPartitionSpec,
      icebergSortOrder)
    val requiredOrdering = Spark3Util.buildRequiredOrdering(
      requiredDistribution,
      icebergSchema,
      icebergPartitionSpec,
      icebergSortOrder)

    (requiredDistribution, requiredOrdering)
  }
}
