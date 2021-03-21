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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

trait IcebergSupport {

  protected def isIcebergRelation(plan: LogicalPlan): Boolean = {
    def isIcebergTable(relation: DataSourceV2Relation): Boolean = relation.table match {
      case _: org.apache.iceberg.spark.source.SparkTable => true
      case _ => false
    }

    plan match {
      case s: SubqueryAlias => isIcebergRelation(s.child)
      case r: DataSourceV2Relation => isIcebergTable(r)
      case _ => false
    }
  }

  protected def isIcebergTable(
      catalog: TableCatalog,
      props: Map[String, String]): Boolean = {

    catalog match {
      case _: org.apache.iceberg.spark.SparkCatalog =>
        true
      case icebergSessionCatalog: org.apache.iceberg.spark.SparkSessionCatalog[_] =>
        val provider = props.getOrElse(TableCatalog.PROP_PROVIDER, "iceberg")
        icebergSessionCatalog.useIceberg(provider)
      case _ =>
        false
    }
  }
}
