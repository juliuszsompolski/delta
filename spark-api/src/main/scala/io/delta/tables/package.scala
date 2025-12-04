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

package io.delta

import org.apache.spark.annotation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

package object tables {

  /**
   * The default implementation companion. This is set by the implementation module
   * (classic or connect) at initialization time.
   */
  @volatile private[tables] var defaultCompanion: DeltaTableCompanion = _

  /**
   * Set the default implementation companion. This should be called by the implementation
   * module during initialization.
   *
   * @param companion the DeltaTableCompanion implementation to use as default
   */
  def setDefaultImplementation(companion: DeltaTableCompanion): Unit = {
    defaultCompanion = companion
  }
}

/**
 * Abstract companion object with factory methods for creating DeltaTable instances.
 * Implementations (classic, connect) provide concrete implementations of these methods.
 *
 * @since 0.3.0
 */
abstract class DeltaTableCompanion {

  /**
   * Create a DeltaTable from the given parquet table and partition schema.
   * Takes an existing parquet table and constructs a delta transaction log in the base path of
   * that table.
   *
   * Note: Any changes to the table during the conversion process may not result in a consistent
   * state at the end of the conversion. Users should stop any changes to the table before the
   * conversion is started.
   *
   * @param spark the SparkSession to use
   * @param identifier the table identifier (e.g., "parquet.`/path`")
   * @param partitionSchema the partition schema as StructType
   * @return the converted DeltaTable
   * @since 0.4.0
   */
  def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: StructType): DeltaTable

  /**
   * Create a DeltaTable from the given parquet table and partition schema.
   * Takes an existing parquet table and constructs a delta transaction log in the base path of
   * that table.
   *
   * Note: Any changes to the table during the conversion process may not result in a consistent
   * state at the end of the conversion. Users should stop any changes to the table before the
   * conversion is started.
   *
   * @param spark the SparkSession to use
   * @param identifier the table identifier (e.g., "parquet.`/path`")
   * @param partitionSchema the partition schema as DDL string (e.g., "key1 long, key2 string")
   * @return the converted DeltaTable
   * @since 0.4.0
   */
  def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: String): DeltaTable

  /**
   * Create a DeltaTable from the given parquet table. Takes an existing parquet table and
   * constructs a delta transaction log in the base path of the table.
   *
   * Note: Any changes to the table during the conversion process may not result in a consistent
   * state at the end of the conversion. Users should stop any changes to the table before the
   * conversion is started.
   *
   * @param spark the SparkSession to use
   * @param identifier the table identifier (e.g., "parquet.`/path`")
   * @return the converted DeltaTable
   * @since 0.4.0
   */
  def convertToDelta(spark: SparkSession, identifier: String): DeltaTable

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path. If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @param path the path to the Delta table
   * @return a DeltaTable for the given path
   * @since 0.3.0
   */
  def forPath(path: String): DeltaTable

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path. If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * @param sparkSession the SparkSession to use
   * @param path the path to the Delta table
   * @return a DeltaTable for the given path
   * @since 0.3.0
   */
  def forPath(sparkSession: SparkSession, path: String): DeltaTable

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path. If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * @param sparkSession the SparkSession to use
   * @param path the path to the Delta table
   * @param hadoopConf Hadoop configuration starting with "fs." or "dfs." will be picked up
   *                   by `DeltaTable` to access the file system when executing queries.
   *                   Other configurations will not be allowed.
   * @return a DeltaTable for the given path
   * @since 2.2.0
   */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: scala.collection.Map[String, String]): DeltaTable

  /**
   * Java friendly API to instantiate a [[DeltaTable]] object representing the data at the given
   * path. If the given path is invalid (i.e. either no table exists or an existing table is not a
   * Delta table), it throws a `not a Delta table` error.
   *
   * @param sparkSession the SparkSession to use
   * @param path the path to the Delta table
   * @param hadoopConf Hadoop configuration starting with "fs." or "dfs." will be picked up
   *                   by `DeltaTable` to access the file system when executing queries.
   *                   Other configurations will be ignored.
   * @return a DeltaTable for the given path
   * @since 2.2.0
   */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: java.util.Map[String, String]): DeltaTable

  /**
   * Instantiate a [[DeltaTable]] object using the given table name. If the given
   * tableOrViewName is invalid (i.e. either no table exists or an existing table is not a
   * Delta table), it throws a `not a Delta table` error. Note: Passing a view name will also
   * result in this error as views are not supported.
   *
   * The given tableOrViewName can also be the absolute path of a delta datasource (i.e.
   * delta.`path`), If so, instantiate a [[DeltaTable]] object representing the data at
   * the given path (consistent with the [[forPath]]).
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @param tableOrViewName the table name or view name
   * @return a DeltaTable for the given table
   * @since 0.3.0
   */
  def forName(tableOrViewName: String): DeltaTable

  /**
   * Instantiate a [[DeltaTable]] object using the given table name and SparkSession. If the given
   * tableOrViewName is invalid (i.e. either no table exists or an existing table is not a
   * Delta table), it throws a `not a Delta table` error. Note: Passing a view name will also
   * result in this error as views are not supported.
   *
   * The given tableOrViewName can also be the absolute path of a delta datasource (i.e.
   * delta.`path`), If so, instantiate a [[DeltaTable]] object representing the data at
   * the given path (consistent with the [[forPath]]).
   *
   * @param sparkSession the SparkSession to use
   * @param tableName the table name
   * @return a DeltaTable for the given table
   * @since 0.3.0
   */
  def forName(sparkSession: SparkSession, tableName: String): DeltaTable

  /**
   * Check if the provided `identifier` string, in this case a file path,
   * is the root of a Delta table using the given SparkSession.
   *
   * @param sparkSession the SparkSession to use
   * @param identifier the path to check
   * @return true if the path is a Delta table, false otherwise
   * @since 0.4.0
   */
  def isDeltaTable(sparkSession: SparkSession, identifier: String): Boolean

  /**
   * Check if the provided `identifier` string, in this case a file path,
   * is the root of a Delta table.
   *
   * Note: This uses the active SparkSession in the current thread to search for the table. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @param identifier the path to check
   * @return true if the path is a Delta table, false otherwise
   * @since 0.4.0
   */
  def isDeltaTable(identifier: String): Boolean

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to create a Delta table,
   * error if the table exists (the same as SQL `CREATE TABLE`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @return a DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def create(): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to create a Delta table,
   * error if the table exists (the same as SQL `CREATE TABLE`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * @param spark sparkSession passed by the user
   * @return a DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def create(spark: SparkSession): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to create a Delta table,
   * if it does not exist (the same as SQL `CREATE TABLE IF NOT EXISTS`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @return a DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def createIfNotExists(): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to create a Delta table,
   * if it does not exist (the same as SQL `CREATE TABLE IF NOT EXISTS`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * @param spark sparkSession passed by the user
   * @return a DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def createIfNotExists(spark: SparkSession): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to replace a Delta table,
   * error if the table doesn't exist (the same as SQL `REPLACE TABLE`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @return a DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def replace(): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to replace a Delta table,
   * error if the table doesn't exist (the same as SQL `REPLACE TABLE`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * @param spark sparkSession passed by the user
   * @return a DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def replace(spark: SparkSession): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to replace a Delta table
   * or create table if not exists (the same as SQL `CREATE OR REPLACE TABLE`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @return a DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def createOrReplace(): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to replace a Delta table,
   * or create table if not exists (the same as SQL `CREATE OR REPLACE TABLE`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * @param spark sparkSession passed by the user
   * @return a DeltaTableBuilder
   * @since 1.0.0
   */
  @Evolving
  def createOrReplace(spark: SparkSession): DeltaTableBuilder

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaColumnBuilder]] to specify a column.
   * Refer to [[DeltaTableBuilder]] for examples and [[DeltaColumnBuilder]] detailed APIs.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @param colName string the column name
   * @return a DeltaColumnBuilder
   * @since 1.0.0
   */
  @Evolving
  def columnBuilder(colName: String): DeltaColumnBuilder

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaColumnBuilder]] to specify a column.
   * Refer to [[DeltaTableBuilder]] for examples and [[DeltaColumnBuilder]] detailed APIs.
   *
   * @param spark sparkSession passed by the user
   * @param colName string the column name
   * @return a DeltaColumnBuilder
   * @since 1.0.0
   */
  @Evolving
  def columnBuilder(spark: SparkSession, colName: String): DeltaColumnBuilder
}

/**
 * Companion object with static factory methods for creating DeltaTable instances.
 * Delegates to the default implementation companion.
 *
 * {{{
 *   DeltaTable.forPath(sparkSession, pathToTheDeltaTable)
 * }}}
 *
 * @since 0.3.0
 */
object DeltaTable {

  private def companion: DeltaTableCompanion = {
    val c = tables.defaultCompanion
    if (c == null) {
      throw new IllegalStateException(
        "No Delta implementation has been initialized. " +
        "Make sure to include delta-spark or delta-connect dependency.")
    }
    c
  }

  /** @inheritdoc */
  def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: StructType): DeltaTable =
    companion.convertToDelta(spark, identifier, partitionSchema)

  /** @inheritdoc */
  def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: String): DeltaTable =
    companion.convertToDelta(spark, identifier, partitionSchema)

  /** @inheritdoc */
  def convertToDelta(spark: SparkSession, identifier: String): DeltaTable =
    companion.convertToDelta(spark, identifier)

  /** @inheritdoc */
  def forPath(path: String): DeltaTable = companion.forPath(path)

  /** @inheritdoc */
  def forPath(sparkSession: SparkSession, path: String): DeltaTable =
    companion.forPath(sparkSession, path)

  /** @inheritdoc */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: scala.collection.Map[String, String]): DeltaTable =
    companion.forPath(sparkSession, path, hadoopConf)

  /** @inheritdoc */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: java.util.Map[String, String]): DeltaTable =
    companion.forPath(sparkSession, path, hadoopConf)

  /** @inheritdoc */
  def forName(tableOrViewName: String): DeltaTable = companion.forName(tableOrViewName)

  /** @inheritdoc */
  def forName(sparkSession: SparkSession, tableName: String): DeltaTable =
    companion.forName(sparkSession, tableName)

  /** @inheritdoc */
  def isDeltaTable(sparkSession: SparkSession, identifier: String): Boolean =
    companion.isDeltaTable(sparkSession, identifier)

  /** @inheritdoc */
  def isDeltaTable(identifier: String): Boolean = companion.isDeltaTable(identifier)

  /** @inheritdoc */
  @Evolving
  def create(): DeltaTableBuilder = companion.create()

  /** @inheritdoc */
  @Evolving
  def create(spark: SparkSession): DeltaTableBuilder = companion.create(spark)

  /** @inheritdoc */
  @Evolving
  def createIfNotExists(): DeltaTableBuilder = companion.createIfNotExists()

  /** @inheritdoc */
  @Evolving
  def createIfNotExists(spark: SparkSession): DeltaTableBuilder =
    companion.createIfNotExists(spark)

  /** @inheritdoc */
  @Evolving
  def replace(): DeltaTableBuilder = companion.replace()

  /** @inheritdoc */
  @Evolving
  def replace(spark: SparkSession): DeltaTableBuilder = companion.replace(spark)

  /** @inheritdoc */
  @Evolving
  def createOrReplace(): DeltaTableBuilder = companion.createOrReplace()

  /** @inheritdoc */
  @Evolving
  def createOrReplace(spark: SparkSession): DeltaTableBuilder = companion.createOrReplace(spark)

  /** @inheritdoc */
  @Evolving
  def columnBuilder(colName: String): DeltaColumnBuilder = companion.columnBuilder(colName)

  /** @inheritdoc */
  @Evolving
  def columnBuilder(spark: SparkSession, colName: String): DeltaColumnBuilder =
    companion.columnBuilder(spark, colName)
}
