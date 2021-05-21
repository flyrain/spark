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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedPartitionSpec, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, GenericInternalRow, NamedExpression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.SupportsFileFilter
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode, LocalTableScanExec, ProjectExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.streaming.continuous.{WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.StorageLevel

class DataSourceV2Strategy(session: SparkSession) extends Strategy with PredicateHelper {

  import DataSourceV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private def withProjectAndFilter(
      project: Seq[NamedExpression],
      filters: Seq[Expression],
      scan: LeafExecNode,
      needsUnsafeConversion: Boolean): SparkPlan = {
    val filterCondition = filters.reduceLeftOption(And)
    val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

    if (withFilter.output != project || needsUnsafeConversion) {
      ProjectExec(project, withFilter)
    } else {
      withFilter
    }
  }

  private def refreshCache(r: DataSourceV2Relation)(): Unit = {
    session.sharedState.cacheManager.recacheByPlan(session, r)
  }

  private def invalidateCache(
      r: ResolvedTable,
      recacheTable: Boolean = false)(): Option[StorageLevel] = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    val cache = session.sharedState.cacheManager.lookupCachedData(v2Relation)
    session.sharedState.cacheManager.uncacheQuery(session, v2Relation, cascade = true)
    if (cache.isDefined) {
      val cacheLevel = cache.get.cachedRepresentation.cacheBuilder.storageLevel

      if (recacheTable) {
        val cacheName = cache.get.cachedRepresentation.cacheBuilder.tableName
        // recache with the same name and cache level.
        val ds = Dataset.ofRows(session, v2Relation)
        session.sharedState.cacheManager.cacheQuery(ds, cacheName, cacheLevel)
      }
      Some(cacheLevel)
    } else {
      None
    }
  }

  private def invalidateCache(catalog: TableCatalog, table: Table, ident: Identifier): Unit = {
    val v2Relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
    session.sharedState.cacheManager.uncacheQuery(session, v2Relation, cascade = true)
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters,
        relation @ DataSourceV2ScanRelation(_, V1ScanWrapper(scan, translated, pushed), output)) =>
      val v1Relation = scan.toV1TableScan[BaseRelation with TableScan](session.sqlContext)
      if (v1Relation.schema != scan.readSchema()) {
        throw new IllegalArgumentException(
          "The fallback v1 relation reports inconsistent schema:\n" +
            "Schema of v2 scan:     " + scan.readSchema() + "\n" +
            "Schema of v1 relation: " + v1Relation.schema)
      }
      val rdd = v1Relation.buildScan()
      val unsafeRowRDD = DataSourceStrategy.toCatalystRDD(v1Relation, output, rdd)
      val dsScan = RowDataSourceScanExec(
        output,
        output.toStructType,
        translated.toSet,
        pushed.toSet,
        unsafeRowRDD,
        v1Relation,
        tableIdentifier = None)
      withProjectAndFilter(project, filters, dsScan, needsUnsafeConversion = false) :: Nil

    case PhysicalOperation(project, filters,
        DataSourceV2ScanRelation(_, scan: SupportsFileFilter, output)) =>
      // similar to the case below but is used for operations with dynamic file filtering,
      // which requires us to disable caching of input splits
      val batchExec = BatchScanExec(output, scan, cachePartitions = false)
      withProjectAndFilter(project, filters, batchExec, !batchExec.supportsColumnar) :: Nil

    case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val batchExec = BatchScanExec(relation.output, relation.scan)
      withProjectAndFilter(project, filters, batchExec, !batchExec.supportsColumnar) :: Nil

    case r: StreamingDataSourceV2Relation if r.startOffset.isDefined && r.endOffset.isDefined =>
      val microBatchStream = r.stream.asInstanceOf[MicroBatchStream]
      val scanExec = MicroBatchScanExec(
        r.output, r.scan, microBatchStream, r.startOffset.get, r.endOffset.get)

      val withProjection = if (scanExec.supportsColumnar) {
        scanExec
      } else {
        // Add a Project here to make sure we produce unsafe rows.
        ProjectExec(r.output, scanExec)
      }

      withProjection :: Nil

    case r: StreamingDataSourceV2Relation if r.startOffset.isDefined && r.endOffset.isEmpty =>
      val continuousStream = r.stream.asInstanceOf[ContinuousStream]
      val scanExec = ContinuousScanExec(r.output, r.scan, continuousStream, r.startOffset.get)

      val withProjection = if (scanExec.supportsColumnar) {
        scanExec
      } else {
        // Add a Project here to make sure we produce unsafe rows.
        ProjectExec(r.output, scanExec)
      }

      withProjection :: Nil

    case WriteToDataSourceV2(writer, query) =>
      WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

    case CreateV2Table(catalog, ident, schema, parts, props, ifNotExists,
        distributionMode, ordering) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      CreateTableExec(catalog, ident, schema, parts, propsWithOwner, ifNotExists,
        distributionMode, ordering) :: Nil

    case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists,
        distributionMode, ordering) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicCreateTableAsSelectExec(staging, ident, parts, query, planLater(query),
            propsWithOwner, writeOptions, ifNotExists, distributionMode, ordering) :: Nil
        case _ =>
          CreateTableAsSelectExec(catalog, ident, parts, query, planLater(query),
            propsWithOwner, writeOptions, ifNotExists, distributionMode, ordering) :: Nil
      }

    case MigrateTable(catalog, ident, props) =>
      MigrateTableExec(catalog, ident, props) :: Nil

    case SnapshotTable(sourceTableCatalog, sourceIdent, tableCatalog, ident, props) =>
      SnapshotTableExec(sourceTableCatalog, sourceIdent, tableCatalog, ident, props) :: Nil

    case RefreshTable(r: ResolvedTable) =>
      RefreshTableExec(r.catalog, r.identifier, invalidateCache(r, recacheTable = true)) :: Nil

    case ReplaceTable(catalog, ident, schema, parts, props, orCreate, distributionMode, ordering) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableExec(
            staging, ident, schema, parts, propsWithOwner, orCreate = orCreate,
            invalidateCache, distributionMode, ordering) :: Nil
        case _ =>
          ReplaceTableExec(
            catalog, ident, schema, parts, propsWithOwner, orCreate = orCreate,
            invalidateCache, distributionMode, ordering) :: Nil
      }

    case ReplaceTableAsSelect(catalog, ident, parts, query, props, options, orCreate,
        distributionMode, ordering) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableAsSelectExec(
            staging,
            ident,
            parts,
            query,
            planLater(query),
            propsWithOwner,
            writeOptions,
            orCreate = orCreate,
            invalidateCache,
            distributionMode,
            ordering) :: Nil
        case _ =>
          ReplaceTableAsSelectExec(
            catalog,
            ident,
            parts,
            query,
            planLater(query),
            propsWithOwner,
            writeOptions,
            orCreate = orCreate,
            invalidateCache,
            distributionMode,
            ordering) :: Nil
      }

    case AppendData(r @ DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), query, writeOptions,
        _, Some(write)) if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          AppendDataExecV1(v1, writeOptions.asOptions, query, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw new AnalysisException(
            s"Table ${v1.name} declares ${TableCapability.V1_BATCH_WRITE} capability but " +
            s"${v2Write.getClass.getName} is not an instance of ${classOf[V1Write].getName}")
      }

    case AppendData(r @ DataSourceV2Relation(v2: SupportsWrite, _, _, _, _), query, writeOptions,
        _, Some(write)) =>
      AppendDataExec(v2, writeOptions.asOptions, planLater(query), refreshCache(r), write) :: Nil

    case OverwriteByExpression(r @ DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), _, query,
        writeOptions, _, Some(write)) if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          OverwriteByExpressionExecV1(
            v1, writeOptions.asOptions, query, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw new AnalysisException(
            s"Table ${v1.name} declares ${TableCapability.V1_BATCH_WRITE} capability but " +
            s"${v2Write.getClass.getName} is not an instance of ${classOf[V1Write].getName}")
      }

    case OverwriteByExpression(r @ DataSourceV2Relation(v2: SupportsWrite, _, _, _, _), _, query,
        writeOptions, _, Some(write)) =>
      OverwriteByExpressionExec(
        v2, writeOptions.asOptions, planLater(query), refreshCache(r), write) :: Nil

    case OverwritePartitionsDynamic(r: DataSourceV2Relation, query, writeOptions, _, Some(write)) =>
      OverwritePartitionsDynamicExec(
        r.table.asWritable, writeOptions.asOptions, planLater(query),
        refreshCache(r), write) :: Nil

    case DeleteFromTable(relation, condition) =>
      relation match {
        case DataSourceV2ScanRelation(r, _, output) =>
          val table = r.table
          if (condition.exists(SubqueryExpression.hasSubquery)) {
            throw new AnalysisException(
              s"Delete by condition with subquery is not supported: $condition")
          }
          // fail if any filter cannot be converted.
          // correctness depends on removing all matching data.
          val filters = DataSourceStrategy.normalizeExprs(condition.toSeq, output)
              .flatMap(splitConjunctivePredicates(_).map {
                f => DataSourceStrategy.translateFilter(f, true).getOrElse(
                  throw new AnalysisException(s"Exec update failed:" +
                      s" cannot translate expression to source filter: $f"))
              }).toArray

          if (!table.asDeletable.canDeleteWhere(filters)) {
            throw new AnalysisException(
              s"Cannot delete from table ${table.name} where ${filters.mkString("[", ", ", "]")}")
          }

          DeleteFromTableExec(table.asDeletable, filters, refreshCache(r)) :: Nil
        case _ =>
          throw new AnalysisException("DELETE is only supported with v2 tables.")
      }

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case DescribeNamespace(ResolvedNamespace(catalog, ns), extended, output) =>
      DescribeNamespaceExec(output, catalog.asNamespaceCatalog, ns, extended) :: Nil

    case DescribeRelation(r: ResolvedTable, partitionSpec, isExtended, output) =>
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException("DESCRIBE does not support partition for v2 tables.")
      }
      DescribeTableExec(output, r.table, isExtended) :: Nil

    case DescribeColumn(_: ResolvedTable, _, _) =>
      throw new AnalysisException("Describing columns is not supported for v2 tables.")

    case DropTable(r: ResolvedTable, ifExists, purge) =>
      DropTableExec(r.catalog, r.identifier, ifExists, purge, invalidateCache(r)) :: Nil

    case _: NoopDropTable =>
      LocalTableScanExec(Nil, Nil) :: Nil

    case AlterTable(catalog, ident, _, changes) =>
      AlterTableExec(catalog, ident, changes) :: Nil

    case RenameTable(catalog, oldIdent, newIdent) =>
      val tbl = ResolvedTable(catalog, oldIdent, catalog.loadTable(oldIdent))
      RenameTableExec(
        catalog,
        oldIdent,
        newIdent,
        invalidateCache(tbl),
        session.sharedState.cacheManager.cacheQuery) :: Nil

    case AlterNamespaceSetProperties(ResolvedNamespace(catalog, ns), properties) =>
      AlterNamespaceSetPropertiesExec(catalog.asNamespaceCatalog, ns, properties) :: Nil

    case AlterNamespaceSetLocation(ResolvedNamespace(catalog, ns), location) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_LOCATION -> location)) :: Nil

    case CommentOnNamespace(ResolvedNamespace(catalog, ns), comment) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_COMMENT -> comment)) :: Nil

    case CommentOnTable(ResolvedTable(catalog, identifier, _), comment) =>
      val changes = TableChange.setProperty(TableCatalog.PROP_COMMENT, comment)
      AlterTableExec(catalog, identifier, Seq(changes)) :: Nil

    case CreateNamespace(catalog, namespace, ifNotExists, properties) =>
      CreateNamespaceExec(catalog, namespace, ifNotExists, properties) :: Nil

    case DropNamespace(ResolvedNamespace(catalog, ns), ifExists, cascade) =>
      DropNamespaceExec(catalog, ns, ifExists, cascade) :: Nil

    case ShowNamespaces(ResolvedNamespace(catalog, ns), pattern, output) =>
      ShowNamespacesExec(output, catalog.asNamespaceCatalog, ns, pattern) :: Nil

    case r @ ShowTables(ResolvedNamespace(catalog, ns), pattern) =>
      ShowTablesExec(r.output, catalog.asTableCatalog, ns, pattern) :: Nil

    case SetCatalogAndNamespace(catalogManager, catalogName, ns) =>
      SetCatalogAndNamespaceExec(catalogManager, catalogName, ns) :: Nil

    case r: ShowCurrentNamespace =>
      ShowCurrentNamespaceExec(r.output, r.catalogManager) :: Nil

    case r @ ShowTableProperties(rt: ResolvedTable, propertyKey) =>
      ShowTablePropertiesExec(r.output, rt.table, propertyKey) :: Nil

    case AnalyzeTable(_: ResolvedTable, _, _) | AnalyzeColumn(_: ResolvedTable, _, _) =>
      throw new AnalysisException("ANALYZE TABLE is not supported for v2 tables.")

    case AlterTableAddPartition(
        ResolvedTable(_, _, table: SupportsPartitionManagement), parts, ignoreIfExists) =>
      AlterTableAddPartitionExec(
        table, parts.asResolvedPartitionSpecs, ignoreIfExists) :: Nil

    case AlterTableDropPartition(
        ResolvedTable(_, _, table: SupportsPartitionManagement), parts, ignoreIfNotExists, _, _) =>
      AlterTableDropPartitionExec(
        table, parts.asResolvedPartitionSpecs, ignoreIfNotExists) :: Nil

    case LoadData(_: ResolvedTable, _, _, _, _) =>
      throw new AnalysisException("LOAD DATA is not supported for v2 tables.")

    case ShowCreateTable(_: ResolvedTable, _) =>
      throw new AnalysisException("SHOW CREATE TABLE is not supported for v2 tables.")

    case TruncateTable(_: ResolvedTable, _) =>
      throw new AnalysisException("TRUNCATE TABLE is not supported for v2 tables.")

    case ShowColumns(_: ResolvedTable, _) =>
      throw new AnalysisException("SHOW COLUMNS is not supported for v2 tables.")

    case r @ ShowPartitions(
        ResolvedTable(catalog, _, table: SupportsPartitionManagement),
        pattern @ (None | Some(_: ResolvedPartitionSpec))) =>
      ShowPartitionsExec(
        r.output,
        catalog,
        table,
        pattern.map(_.asInstanceOf[ResolvedPartitionSpec])) :: Nil

    case c @ Call(procedure, args) =>
      val input = buildInternalRow(args)
      CallExec(c.output, procedure, input) :: Nil

    case DynamicFileFilter(scanPlan, fileFilterPlan, filterable) =>
      DynamicFileFilterExec(planLater(scanPlan), planLater(fileFilterPlan), filterable) :: Nil

    case DynamicFileFilterWithCardinalityCheck(scanPlan, fileFilterPlan, filterable, accumulator) =>
      DynamicFileFilterWithCardinalityCheckExec(
        planLater(scanPlan),
        planLater(fileFilterPlan),
        filterable,
        accumulator) :: Nil

    case ReplaceData(r: DataSourceV2Relation, query, write) =>
      ReplaceDataExec(r.table.asMergeable, planLater(query), refreshCache(r), write) :: Nil

    case MergeInto(mergeIntoParams, output, child) =>
      MergeIntoExec(mergeIntoParams, output, planLater(child)) :: Nil

    case OptimizeTable(relation, predicate, strategy, options) =>
      val (table, output) = relation match {
        case DataSourceV2Relation(table, output, _, _, _) =>
          table match {
            case optimizable: SupportsOptimize =>
              (optimizable, output)
            case other =>
              throw new AnalysisException(s"OPTIMIZE is not supported by table $other")
          }
        case _ =>
          throw new AnalysisException("OPTIMIZE is only supported for v2 tables")
      }
      val predicates = splitConjunctivePredicates(predicate)
      val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, output)
      val filters = toDataSourceFilters(normalizedPredicates)
      OptimizeTableExec(table, filters, strategy, options) :: Nil

    case _ => Nil
  }

  private def buildInternalRow(exprs: Seq[Expression]): InternalRow = {
    val values = new Array[Any](exprs.size)
    for (index <- exprs.indices) {
      values(index) = exprs(index).eval()
    }
    new GenericInternalRow(values)
  }

  private def toDataSourceFilters(predicates: Seq[Expression]): Seq[sources.Filter] = {
    predicates.flatMap { predicate =>
      val translatedFilter = DataSourceStrategy.translateFilter(
        predicate,
        supportNestedPredicatePushdown = true)

      if (translatedFilter.isEmpty) {
        throw new AnalysisException(s"Could not translate $predicate to a data source filter")
      }

      translatedFilter
    }
  }
}
