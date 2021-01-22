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

package org.apache.spark.sql.connector

import java.util

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsMigrate, SupportsSnapshot, Table, TableCatalog}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DataSourceV2SQLExtensionsSuite extends SparkFunSuite with SharedSparkSession
  with BeforeAndAfter  {

  before {
    spark.conf.set("spark.sql.catalog.spark_catalog", classOf[MigrateTestCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat", classOf[MigrateTestCatalog].getName)
    spark.conf.set("spark.sql.catalog.inmemcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalog.reset()
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    sql("DROP TABLE IF EXISTS testcat.t1")
    sql("DROP TABLE IF EXISTS testcat.t1_migrate")
    sql("DROP TABLE IF EXISTS testcat.t1_snapshot")
    sql("CREATE TABLE testcat.t1 (id INT) USING orc")
    sql("CREATE TABLE t1 (id INT) USING orc")
  }

  val tableIdent = Identifier.of(Array.empty, "t1")
  val tableMigratedIdent = Identifier.of(Array.empty, "t1_migrate")
  val tableSnapshotIdent = Identifier.of(Array.empty, "t1_snapshot")

  def catalog() : TableCatalog = {
    spark.sessionState.catalogManager.catalog("testcat").asInstanceOf[TableCatalog]
  }

  def sessionCatalog() : TableCatalog = {
    spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[TableCatalog]
  }

  test("Migrate Table: No Args") {
    sql("MIGRATE TABLE testcat.t1")
    catalog().loadTable(tableMigratedIdent)
  }

  test("Migrate Table: With Format") {
    sql("MIGRATE TABLE testcat.t1 USING parquet")
    val properties = catalog().loadTable(tableMigratedIdent).properties
    assert(properties.get(TableCatalog.PROP_PROVIDER) == "parquet")
  }

  test("Migrate Table: With Properties") {
    sql("MIGRATE TABLE testcat.t1 TBLPROPERTIES ('foo' 'bar')")
    val properties = catalog().loadTable(tableMigratedIdent).properties
    assert(properties.get("foo") == "bar")
  }

  test("Snapshot Table: No Args") {
    sql("SNAPSHOT TABLE testcat.t1 AS testcat.t1_snapshot")
    catalog().loadTable(tableSnapshotIdent)
  }

  test("Snapshot Table: With Location") {
    sql("SNAPSHOT TABLE testcat.t1 AS testcat.t1_snapshot LOCATION 'foo'")
    val properties = catalog().loadTable(tableSnapshotIdent).properties()
    assert(properties.get(TableCatalog.PROP_LOCATION) == "foo")
  }

  test("Snapshot Table: With Properties") {
    sql("SNAPSHOT TABLE testcat.t1 AS testcat.t1_snapshot TBLPROPERTIES ('foo' 'bar')")
    val properties = catalog().loadTable(tableSnapshotIdent).properties()
    assert(properties.get("foo") == "bar")
  }

  test("Snapshot Table: With Format") {
    sql("SNAPSHOT TABLE testcat.t1 AS testcat.t1_snapshot USING parquet")
    val properties = catalog().loadTable(tableSnapshotIdent).properties()
    assert(properties.get(TableCatalog.PROP_PROVIDER) == "parquet")
  }

  test("Migrate Table: Session Catalog") {
    sql("MIGRATE TABLE t1")
    sessionCatalog().loadTable(Identifier.of(Array("default"), "t1_migrate"))
  }

  test("Snapshot Table: Session Catalog") {
    sql("SNAPSHOT TABLE t1 AS t1_snapshot")
    sessionCatalog().loadTable(Identifier.of(Array("default"), "t1_snapshot"))
  }

  test("Analysis Exceptions for Catalogs without Support") {
    intercept[AnalysisException] {
      sql("SNAPSHOT TABLE inmemcat.t1 AS inmemcat.t1_snapshot USING parquet")
    }
    intercept[AnalysisException] {
      sql("MIGRATE TABLE inmemcat.t1")
    }
  }
}

class MigrateTestCatalog extends InMemoryTableCatalog with SupportsMigrate with SupportsSnapshot {

  /**
   * A Dummy method for testing resolution and parsing
   */
  override def migrateTable(
      ident: Identifier,
      properties: util.Map[String, String]): Table = {

    this.createTable(
      Identifier.of(ident.namespace(), ident.name() + "_migrate"),
      StructType(Seq.empty),
      Array.empty,
      properties)
  }

  /**
   * A Dummy method for testing resolution and parsing
   */
  override def snapshotTable(
      sourceTableCatalog: TableCatalog,
      sourceIdent: Identifier,
      ident: Identifier,
      properties: util.Map[String, String]): Table = {

    this.createTable(
      ident,
      StructType(Seq.empty),
      Array.empty,
      properties
    )
  }
}
