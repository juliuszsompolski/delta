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

package io.delta.tables

import org.apache.spark.annotation._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * :: Evolving ::
 *
 * Builder to specify how to create / replace a Delta table.
 * You must specify the table name or the path before executing the builder.
 * You can specify the table columns, the partitioning columns, the location of the data,
 * the table comment and the property, and how you want to create / replace the Delta table.
 *
 * After executing the builder, an instance of [[DeltaTable]] is returned.
 *
 * Scala example to create a Delta table with generated columns, using the table name:
 * {{{
 *   val table: DeltaTable = DeltaTable.create()
 *     .tableName("testTable")
 *     .addColumn("c1",  dataType = "INT", nullable = false)
 *     .addColumn(
 *       DeltaTable.columnBuilder("c2")
 *         .dataType("INT")
 *         .generatedAlwaysAs("c1 + 10")
 *         .build()
 *     )
 *     .addColumn(
 *       DeltaTable.columnBuilder("c3")
 *         .dataType("INT")
 *         .comment("comment")
 *         .nullable(true)
 *         .build()
 *     )
 *     .partitionedBy("c1", "c2")
 *     .execute()
 * }}}
 *
 * Scala example to create a delta table using the location:
 * {{{
 *   val table: DeltaTable = DeltaTable.createIfNotExists(spark)
 *     .location("/foo/bar")
 *     .addColumn("c1", dataType = "INT", nullable = false)
 *     .addColumn(
 *       DeltaTable.columnBuilder(spark, "c2")
 *         .dataType("INT")
 *         .generatedAlwaysAs("c1 + 10")
 *         .build()
 *     )
 *     .addColumn(
 *       DeltaTable.columnBuilder(spark, "c3")
 *         .dataType("INT")
 *         .comment("comment")
 *         .nullable(true)
 *         .build()
 *     )
 *     .partitionedBy("c1", "c2")
 *     .execute()
 * }}}
 *
 * Java Example to replace a table:
 * {{{
 *   DeltaTable table = DeltaTable.replace()
 *     .tableName("db.table")
 *     .addColumn("c1",  "INT", false)
 *     .addColumn(
 *       DeltaTable.columnBuilder("c2")
 *         .dataType("INT")
 *         .generatedAlwaysBy("c1 + 10")
 *         .build()
 *     )
 *     .execute();
 * }}}
 *
 * @since 1.0.0
 */
@Evolving
abstract class DeltaTableBuilder {

  /**
   * :: Evolving ::
   *
   * Specify the table name, optionally qualified with a database name [database_name.] table_name
   *
   * @param identifier string the table name
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def tableName(identifier: String): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify the table comment to describe the table.
   *
   * @param comment string table comment
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def comment(comment: String): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify the path to the directory where table data is stored,
   * which could be a path on distributed storage.
   *
   * @param location string the data location
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def location(location: String): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param colName string the column name
   * @param dataType string the DDL data type
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def addColumn(colName: String, dataType: String): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param colName string the column name
   * @param dataType dataType the DDL data type
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def addColumn(colName: String, dataType: DataType): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param colName string the column name
   * @param dataType string the DDL data type
   * @param nullable boolean whether the column is nullable
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def addColumn(colName: String, dataType: String, nullable: Boolean): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param colName string the column name
   * @param dataType dataType the DDL data type
   * @param nullable boolean whether the column is nullable
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def addColumn(colName: String, dataType: DataType, nullable: Boolean): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param col structField the column struct
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def addColumn(col: StructField): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify columns with an existing schema.
   *
   * @param cols structType the existing schema for columns
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def addColumns(cols: StructType): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify the columns to partition the output on the file system.
   *
   * Note: This should only include table columns already defined in schema.
   *
   * @param colNames column names for partitioning
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  @scala.annotation.varargs
  def partitionedBy(colNames: String*): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify the columns to cluster the output on the file system.
   *
   * Note: This should only include table columns already defined in schema.
   *
   * @param colNames column names for clustering
   * @return this DeltaTableBuilder
   * @since 3.2.0
   */
  @Evolving
  @scala.annotation.varargs
  def clusterBy(colNames: String*): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Specify a key-value pair to tag the table definition.
   *
   * @param key string the table property key
   * @param value string the table property value
   * @return this DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def property(key: String, value: String): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Execute the command to create / replace a Delta table and returns a instance of [[DeltaTable]].
   *
   * @return the created/replaced DeltaTable
   * @since 1.0.0
   */
  @Evolving
  def execute(): DeltaTable
}

/**
 * Options for DeltaTableBuilder to specify table creation mode.
 */
sealed trait DeltaTableBuilderOptions

/**
 * Options for creating a new table.
 * @param ifNotExists if true, create the table only if it doesn't exist
 */
case class CreateTableOptions(ifNotExists: Boolean) extends DeltaTableBuilderOptions

/**
 * Options for replacing a table.
 * @param orCreate if true, create the table if it doesn't exist
 */
case class ReplaceTableOptions(orCreate: Boolean) extends DeltaTableBuilderOptions
