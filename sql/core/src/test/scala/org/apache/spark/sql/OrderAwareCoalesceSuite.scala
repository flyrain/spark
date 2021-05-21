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

package org.apache.spark.sql

import org.apache.spark.rdd.{PartitionCoalescer, PartitionGroup, RDD}
import org.apache.spark.sql.catalyst.plans.logical.OrderAwareCoalesce
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.test.SharedSparkSession

class OrderAwareCoalesceSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("coalesce partitions while preserving the order") {
    val df = Seq(1, 2, 3, 4).toDF("id")
    val repartitionedDF = df.repartitionByRange(4, $"id")
    val coalescer = new PartitionCoalescer with Serializable {
      override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
        require(maxPartitions == 2)
        require(parent.partitions.length == 4)

        val p1 = new PartitionGroup()
        p1.partitions += parent.partitions(0)
        p1.partitions += parent.partitions(1)

        val p2 = new PartitionGroup()
        p2.partitions += parent.partitions(2)
        p2.partitions += parent.partitions(3)

        Array(p1, p2)
      }
    }
    val plan = OrderAwareCoalesce(2, coalescer, repartitionedDF.queryExecution.analyzed)
    val coalescedDF = Dataset.ofRows(spark, plan)
    assert(coalescedDF.rdd.getNumPartitions == 2, "must be 2 partitions")

    val partitionRangeDS = coalescedDF
      .selectExpr("spark_partition_id() p", "id")
      .groupBy("p")
      .agg(min("id"), max("id"))
      .as[(Int, Int, Int)]
    checkDataset(partitionRangeDS, (0, 1, 2), (1, 3, 4))
  }
}
