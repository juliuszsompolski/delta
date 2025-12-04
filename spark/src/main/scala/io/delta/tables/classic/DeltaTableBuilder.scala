/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.tables.classic

import scala.collection.mutable

import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableUtils, TableSpecUtils}
import org.apache.spark.sql.delta.DeltaTableUtils.withActiveSession
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.tables.{
  DeltaTable,
  DeltaTableBuilderOptions,
  CreateTableOptions,
  ReplaceTableOptions
}

import org.apache.spark.annotation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{
  ColumnDefinitionShims,
  CreateTable,
  ReplaceTable
}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Classic (local Spark) implementation of DeltaTableBuilder.
 */
@Evolving
class DeltaTableBuilder private[tables](
    spark: SparkSession,
    builderOption: DeltaTableBuilderOptions)
  extends io.delta.tables.DeltaTableBuilder {

  private var identifier: String = null
  private var partitioningColumns: Option[Seq[String]] = None
  private var clusteringColumns: Option[Seq[String]] = None
  private var columns: mutable.Seq[StructField] = mutable.Seq.empty
  private var tableLocation: Option[String] = None
  private var tblComment: Option[String] = None
  private var properties =
    if (spark.sessionState.conf.getConf(DeltaSQLConf.TABLE_BUILDER_FORCE_TABLEPROPERTY_LOWERCASE)) {
      CaseInsensitiveMap(Map.empty[String, String])
    } else {
      Map.empty[String, String]
    }

  private val FORMAT_NAME: String = "delta"

  /** @inheritdoc */
  @Evolving
  override def tableName(identifier: String): DeltaTableBuilder = {
    this.identifier = identifier
    this
  }

  /** @inheritdoc */
  @Evolving
  override def comment(comment: String): DeltaTableBuilder = {
    tblComment = Option(comment)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def location(location: String): DeltaTableBuilder = {
    this.tableLocation = Option(location)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def addColumn(colName: String, dataType: String): DeltaTableBuilder = {
    addColumn(
      io.delta.tables.classic.DeltaTable.columnBuilder(spark, colName).dataType(dataType).build()
    )
    this
  }

  /** @inheritdoc */
  @Evolving
  override def addColumn(colName: String, dataType: DataType): DeltaTableBuilder = {
    addColumn(
      io.delta.tables.classic.DeltaTable.columnBuilder(spark, colName).dataType(dataType).build()
    )
    this
  }

  /** @inheritdoc */
  @Evolving
  override def addColumn(colName: String, dataType: String, nullable: Boolean): DeltaTableBuilder = {
    addColumn(
      io.delta.tables.classic.DeltaTable
        .columnBuilder(spark, colName).dataType(dataType).nullable(nullable).build()
    )
    this
  }

  /** @inheritdoc */
  @Evolving
  override def addColumn(
      colName: String,
      dataType: DataType,
      nullable: Boolean): DeltaTableBuilder = {
    addColumn(
      io.delta.tables.classic.DeltaTable
        .columnBuilder(spark, colName).dataType(dataType).nullable(nullable).build()
    )
    this
  }

  /** @inheritdoc */
  @Evolving
  override def addColumn(col: StructField): DeltaTableBuilder = {
    columns = columns :+ col
    this
  }

  /** @inheritdoc */
  @Evolving
  override def addColumns(cols: StructType): DeltaTableBuilder = {
    columns = columns ++ cols.toSeq
    this
  }

  /**
   * Validate that clusterBy is not used with partitionedBy.
   */
  private def validatePartitioning(): Unit = {
    if (partitioningColumns.nonEmpty && clusteringColumns.nonEmpty) {
      throw DeltaErrors.clusterByWithPartitionedBy()
    }
  }

  /** @inheritdoc */
  @Evolving
  @scala.annotation.varargs
  override def partitionedBy(colNames: String*): DeltaTableBuilder = {
    partitioningColumns = Option(colNames)
    validatePartitioning()
    this
  }

  /** @inheritdoc */
  @Evolving
  @scala.annotation.varargs
  override def clusterBy(colNames: String*): DeltaTableBuilder = {
    clusteringColumns = Option(colNames)
    validatePartitioning()
    this
  }

  /** @inheritdoc */
  @Evolving
  override def property(key: String, value: String): DeltaTableBuilder = {
    this.properties = this.properties + (key -> value)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def execute(): DeltaTable = withActiveSession(spark) {
    if (identifier == null && tableLocation.isEmpty) {
      throw DeltaErrors.createTableMissingTableNameOrLocation()
    }

    if (this.identifier == null) {
      identifier = s"delta.`${tableLocation.get}`"
    }

    // Return DeltaTable Object.
    val tableId: TableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(identifier)

    if (DeltaTableUtils.isValidPath(tableId) && tableLocation.nonEmpty
        && tableId.table != tableLocation.get) {
      throw DeltaErrors.createTableIdentifierLocationMismatch(identifier, tableLocation.get)
    }

    val table = spark.sessionState.sqlParser.parseMultipartIdentifier(identifier)

    val partitioning = partitioningColumns.map { colNames =>
      colNames.map(name => DeltaTableUtils.parseColToTransform(name))
    }.getOrElse(Seq.empty[Transform]) ++ (clusteringColumns.map { colNames =>
      DeltaTableUtils.parseColsToClusterByTransform(colNames)
    })

    val tableSpec = TableSpecUtils.create(
      properties = properties,
      provider = Some(FORMAT_NAME),
      location = tableLocation,
      comment = tblComment)

    val stmt = builderOption match {
      case CreateTableOptions(ifNotExists) =>
        val unresolvedTable = org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier(table)
        CreateTable(
          unresolvedTable,
          ColumnDefinitionShims.parseColumns(columns.toSeq, spark.sessionState.sqlParser),
          partitioning,
          tableSpec,
          ifNotExists)
      case ReplaceTableOptions(orCreate) =>
        val unresolvedTable = org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier(table)
        ReplaceTable(
          unresolvedTable,
          ColumnDefinitionShims.parseColumns(columns.toSeq, spark.sessionState.sqlParser),
          partitioning,
          tableSpec,
          orCreate)
    }
    val qe = spark.sessionState.executePlan(stmt)
    // call `QueryExecution.toRDD` to trigger the execution of commands.
    SQLExecution.withNewExecutionId(qe, Some("create delta table"))(qe.toRdd)

    // Return DeltaTable Object.
    if (DeltaTableUtils.isValidPath(tableId)) {
      io.delta.tables.classic.DeltaTable.forPath(spark, tableLocation.get)
    } else {
      io.delta.tables.classic.DeltaTable.forName(spark, this.identifier)
    }
  }
}
