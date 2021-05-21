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

package org.apache.spark.sql.execution.command

import java.util.Collections

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, EmptyFunctionRegistry, NoSuchTableException, ResolveSessionCatalog}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, IntegerLiteral, LessThan, Literal, StringLiteral}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{BinPack, LocalRelation, LogicalPlan, OptimizeTable, OrderBy}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.FakeV2Provider
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, Identifier, Table, TableCapability, TableCatalog, V1Table}
import org.apache.spark.sql.connector.expressions.{FieldReference, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{bucket, identity, sort}
import org.apache.spark.sql.connector.expressions.NullOrdering.{NULLS_FIRST, NULLS_LAST}
import org.apache.spark.sql.connector.expressions.SortDirection.{ASCENDING, DESCENDING}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.SimpleScanSource
import org.apache.spark.sql.types.StructType

/**
 * A suite for testing resolution of custom commands.
 */
class ExtendedPlanResolutionSuite extends AnalysisTest {

  private val v1Format = classOf[SimpleScanSource].getName
  private val v2Format = classOf[FakeV2Provider].getName

  private val table: Table = {
    val t = mock(classOf[Table])
    when(t.schema()).thenReturn(new StructType().add("i", "int").add("s", "string"))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val tableWithAcceptAnySchemaCapability: Table = {
    val t = mock(classOf[Table])
    when(t.schema()).thenReturn(new StructType().add("i", "int"))
    when(t.capabilities()).thenReturn(Collections.singleton(TableCapability.ACCEPT_ANY_SCHEMA))
    t
  }

  private val v1Table: V1Table = {
    val t = mock(classOf[CatalogTable])
    when(t.schema).thenReturn(new StructType().add("i", "int").add("s", "string"))
    when(t.tableType).thenReturn(CatalogTableType.MANAGED)
    when(t.provider).thenReturn(Some(v1Format))
    V1Table(t)
  }

  private val v1HiveTable: V1Table = {
    val t = mock(classOf[CatalogTable])
    when(t.schema).thenReturn(new StructType().add("i", "int").add("s", "string"))
    when(t.tableType).thenReturn(CatalogTableType.MANAGED)
    when(t.provider).thenReturn(Some("hive"))
    V1Table(t)
  }

  private val testCat: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[Identifier](0).name match {
        case "tab" => table
        case "tab1" => table
        case name => throw new NoSuchTableException(name)
      }
    })
    when(newCatalog.name()).thenReturn("testcat")
    newCatalog
  }

  private val v2SessionCatalog: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[Identifier](0).name match {
        case "v1Table" => v1Table
        case "v1Table1" => v1Table
        case "v1HiveTable" => v1HiveTable
        case "v2Table" => table
        case "v2Table1" => table
        case "v2TableWithAcceptAnySchemaCapability" => tableWithAcceptAnySchemaCapability
        case name => throw new NoSuchTableException(name)
      }
    })
    when(newCatalog.name()).thenReturn(CatalogManager.SESSION_CATALOG_NAME)
    newCatalog
  }

  private val v1SessionCatalog: SessionCatalog = new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true))
  v1SessionCatalog.createTempView("v", LocalRelation(Nil), false)

  private val catalogManagerWithDefault = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[String](0) match {
        case "testcat" =>
          testCat
        case CatalogManager.SESSION_CATALOG_NAME =>
          v2SessionCatalog
        case name =>
          throw new CatalogNotFoundException(s"No such catalog: $name")
      }
    })
    when(manager.currentCatalog).thenReturn(testCat)
    when(manager.currentNamespace).thenReturn(Array.empty[String])
    when(manager.v1SessionCatalog).thenReturn(v1SessionCatalog)
    manager
  }

  private val catalogManagerWithoutDefault = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[String](0) match {
        case "testcat" =>
          testCat
        case name =>
          throw new CatalogNotFoundException(s"No such catalog: $name")
      }
    })
    when(manager.currentCatalog).thenReturn(v2SessionCatalog)
    when(manager.currentNamespace).thenReturn(Array("default"))
    when(manager.v1SessionCatalog).thenReturn(v1SessionCatalog)
    manager
  }

  def parseAndResolve(query: String, withDefault: Boolean = false): LogicalPlan = {
    val catalogManager = if (withDefault) {
      catalogManagerWithDefault
    } else {
      catalogManagerWithoutDefault
    }
    val analyzer = new Analyzer(catalogManager) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Seq(
        new ResolveSessionCatalog(catalogManager, _ == Seq("v"), _ => false))
    }
    // We don't check analysis here, as we expect the plan to be unresolved such as `CreateTable`.
    analyzer.execute(CatalystSqlParser.parsePlan(query))
  }

  test("optimize (default)") {
    Seq("v2Table", "testcat.tab").foreach { tableName =>
      val sql = s"OPTIMIZE TABLE $tableName"

      parseAndResolve(sql) match {
        case OptimizeTable(_: DataSourceV2Relation, predicate, strategy, options) =>
          assert(predicate == Literal.TrueLiteral, "predicate must be valid")
          assert(strategy == BinPack, "strategy must be valid")
          assert(options.isEmpty, "options must be valid")
        case other =>
          fail(s"Expected ${classOf[OptimizeTable].getName} but got ${other.getClass.getName}")
      }
    }
  }

  test("optimize (default with predicate)") {
    Seq("v2Table", "testcat.tab").foreach { tableName =>
      val sql = s"OPTIMIZE TABLE $tableName WHERE s = 'key'"

      parseAndResolve(sql) match {
        case OptimizeTable(_: DataSourceV2Relation, predicate, strategy, options) =>
          predicate match {
            case EqualTo(_: AttributeReference, StringLiteral("key")) =>
            case _ => fail("predicate must be valid")
          }
          assert(strategy == BinPack, "strategy must be valid")
          assert(options.isEmpty, "options must be valid")
        case other =>
          fail(s"Expected ${classOf[OptimizeTable].getName} but got ${other.getClass.getName}")
      }
    }
  }

  test("optimize (default with options)") {
    Seq("v2Table", "testcat.tab").foreach { tableName =>
      val sql = s"OPTIMIZE $tableName OPTIONS ('p1'='v1', 'p2'='v2')"

      val expectedOptions = Map("p1" -> "v1", "p2" -> "v2")

      parseAndResolve(sql) match {
        case OptimizeTable(_: DataSourceV2Relation, predicate, strategy, options) =>
          assert(predicate == Literal.TrueLiteral, "predicate must be valid")
          assert(strategy == BinPack, "strategy must be valid")
          assert(options == expectedOptions, "options must be valid")
        case other =>
          fail(s"Expected ${classOf[OptimizeTable].getName} but got ${other.getClass.getName}")
      }
    }
  }

  test("optimize (bin-pack with predicate and options)") {
    Seq("v2Table", "testcat.tab").foreach { tableName =>
      val sql = s"OPTIMIZE $tableName WHERE i < 10 BINPACK OPTIONS ('p1'='v1', 'p2'='v2')"

      val expectedOptions = Map("p1" -> "v1", "p2" -> "v2")

      parseAndResolve(sql) match {
        case OptimizeTable(_: DataSourceV2Relation, predicate, strategy, options) =>
          predicate match {
            case LessThan(_: AttributeReference, IntegerLiteral(10)) =>
            case _ => fail("predicate must be valid")
          }
          assert(strategy == BinPack, "strategy must be valid")
          assert(options == expectedOptions, "options must be valid")
        case other =>
          fail(s"Expected ${classOf[OptimizeTable].getName} but got ${other.getClass.getName}")
      }
    }
  }

  test("optimize (default order)") {
    Seq("v2Table", "testcat.tab").foreach { tableName =>
      val sql = s"OPTIMIZE $tableName ORDER"

      parseAndResolve(sql) match {
        case OptimizeTable(_: DataSourceV2Relation, predicate, strategy, options) =>
          assert(predicate == Literal.TrueLiteral, "predicate must be valid")
          assert(strategy == OrderBy(Seq.empty), "strategy must be")
          assert(options.isEmpty, "options must be valid")
        case other =>
          fail(s"Expected ${classOf[OptimizeTable].getName} but got ${other.getClass.getName}")
      }
    }
  }

  test("optimize (default sort with predicate)") {
    Seq("v2Table", "testcat.tab").foreach { tableName =>
      val sql = s"OPTIMIZE $tableName WHERE i < 10 SORT"

      parseAndResolve(sql) match {
        case OptimizeTable(_: DataSourceV2Relation, predicate, strategy, options) =>
          predicate match {
            case LessThan(_: AttributeReference, IntegerLiteral(10)) =>
            case _ => fail("predicate must be valid")
          }
          assert(strategy == OrderBy(Seq.empty), "strategy must be valid")
          assert(options.isEmpty, "options must be valid")
        case other =>
          fail(s"Expected ${classOf[OptimizeTable].getName} but got ${other.getClass.getName}")
      }
    }
  }

  test("optimize (custom sort with predicate and options)") {
    Seq("v2Table", "testcat.tab").foreach { tableName =>
      val sql = s"OPTIMIZE $tableName WHERE i < 10 SORT BY i, bucket(8, s) OPTIONS ('p'='v')"

      val expectedOrdering = Seq[SortOrder](
        sort(identity(FieldReference("i")), ASCENDING, NULLS_FIRST),
        sort(bucket(8, Array(FieldReference("s"))), ASCENDING, NULLS_FIRST)
      )

      val expectedOptions = Map("p" -> "v")

      parseAndResolve(sql) match {
        case OptimizeTable(_: DataSourceV2Relation, predicate, strategy, options) =>
          predicate match {
            case LessThan(_: AttributeReference, IntegerLiteral(10)) =>
            case _ => fail("predicate must be valid")
          }
          assert(strategy == OrderBy(expectedOrdering), "strategy must be valid")
          assert(options == expectedOptions, "options must be valid")
        case other =>
          fail(s"Expected ${classOf[OptimizeTable].getName} but got ${other.getClass.getName}")
      }
    }
  }

  test("optimize (custom order with predicate and options)") {
    Seq("v2Table", "testcat.tab").foreach { tableName =>
      val sql = s"OPTIMIZE $tableName WHERE i < 10 ORDER BY i DESC, bucket(8, s) OPTIONS ('p'='v')"

      val expectedOrdering = Seq[SortOrder](
        sort(identity(FieldReference("i")), DESCENDING, NULLS_LAST),
        sort(bucket(8, Array(FieldReference("s"))), ASCENDING, NULLS_FIRST)
      )

      val expectedOptions = Map("p" -> "v")

      parseAndResolve(sql) match {
        case OptimizeTable(_: DataSourceV2Relation, predicate, strategy, options) =>
          predicate match {
            case LessThan(_: AttributeReference, IntegerLiteral(10)) =>
            case _ => fail("predicate must be valid")
          }
          assert(strategy == OrderBy(expectedOrdering), "strategy must be valid")
          assert(options == expectedOptions, "options must be valid")
        case other =>
          fail(s"Expected ${classOf[OptimizeTable].getName} but got ${other.getClass.getName}")
      }
    }
  }
}
