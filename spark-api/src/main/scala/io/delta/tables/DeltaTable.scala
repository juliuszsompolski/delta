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
import org.apache.spark.sql._

/**
 * Main class for programmatically interacting with Delta tables.
 * You can create DeltaTable instances using the static methods.
 *
 * {{{
 *   DeltaTable.forPath(sparkSession, pathToTheDeltaTable)
 * }}}
 *
 * @since 0.3.0
 */
abstract class DeltaTable extends Serializable {

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @param alias the table alias
   * @return aliased DeltaTable
   * @since 0.3.0
   */
  def as(alias: String): DeltaTable

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @param alias the table alias
   * @return aliased DeltaTable
   * @since 0.3.0
   */
  def alias(alias: String): DeltaTable

  /**
   * Get a DataFrame (that is, Dataset[Row]) representation of this Delta table.
   *
   * @return DataFrame representation of this Delta table
   * @since 0.3.0
   */
  def toDF: Dataset[Row]

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * @param retentionHours The retention threshold in hours. Files required by the table for
   *                       reading versions earlier than this will be preserved and the
   *                       rest of them will be deleted.
   * @return DataFrame containing vacuum statistics
   * @since 0.3.0
   */
  def vacuum(retentionHours: Double): DataFrame

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * Note: This will use the default retention period of 7 days.
   *
   * @return DataFrame containing vacuum statistics
   * @since 0.3.0
   */
  def vacuum(): DataFrame

  /**
   * Get the information of the latest `limit` commits on this table as a Spark DataFrame.
   * The information is in reverse chronological order.
   *
   * @param limit The number of previous commands to get history for
   * @return DataFrame containing commit history
   * @since 0.3.0
   */
  def history(limit: Int): DataFrame

  /**
   * Get the information available commits on this table as a Spark DataFrame.
   * The information is in reverse chronological order.
   *
   * @return DataFrame containing commit history
   * @since 0.3.0
   */
  def history(): DataFrame

  /**
   * :: Evolving ::
   *
   * Get the details of a Delta table such as the format, name, and size.
   *
   * @return DataFrame containing table details
   * @since 2.1.0
   */
  @Evolving
  def detail(): DataFrame

  /**
   * Generate a manifest for the given Delta Table.
   *
   * @param mode Specifies the mode for the generation of the manifest.
   *             The valid modes are as follows (not case sensitive):
   *              - "symlink_format_manifest": This will generate manifests in symlink format
   *                                           for Presto and Athena read support.
   *             See the online documentation for more information.
   * @since 0.5.0
   */
  def generate(mode: String): Unit

  /**
   * Delete data from the table that match the given `condition`.
   *
   * @param condition Boolean SQL expression
   * @since 0.3.0
   */
  def delete(condition: String): Unit

  /**
   * Delete data from the table that match the given `condition`.
   *
   * @param condition Boolean SQL expression
   * @since 0.3.0
   */
  def delete(condition: Column): Unit

  /**
   * Delete all data from the table.
   *
   * @since 0.3.0
   */
  def delete(): Unit

  /**
   * Update rows in the table based on the rules defined by `set`.
   *
   * Scala example to increment the column `data`:
   * {{{
   *    import org.apache.spark.sql.functions._
   *
   *    deltaTable.update(Map("data" -> col("data") + 1))
   * }}}
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 0.3.0
   */
  def update(set: Map[String, Column]): Unit

  /**
   * Update rows in the table based on the rules defined by `set`.
   *
   * Java example to increment the column `data`:
   * {{{
   *    import org.apache.spark.sql.Column;
   *    import org.apache.spark.sql.functions;
   *
   *    deltaTable.update(
   *      new HashMap<String, Column>() {{
   *        put("data", functions.col("data").plus(1));
   *      }}
   *    );
   * }}}
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 0.3.0
   */
  def update(set: java.util.Map[String, Column]): Unit

  /**
   * Update data from the table on the rows that match the given `condition`
   * based on the rules defined by `set`.
   *
   * Scala example to increment the column `data`:
   * {{{
   *    import org.apache.spark.sql.functions._
   *
   *    deltaTable.update(
   *      col("date") > "2018-01-01",
   *      Map("data" -> col("data") + 1))
   * }}}
   *
   * @param condition boolean expression as Column object specifying which rows to update.
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 0.3.0
   */
  def update(condition: Column, set: Map[String, Column]): Unit

  /**
   * Update data from the table on the rows that match the given `condition`
   * based on the rules defined by `set`.
   *
   * Java example to increment the column `data`:
   * {{{
   *    import org.apache.spark.sql.Column;
   *    import org.apache.spark.sql.functions;
   *
   *    deltaTable.update(
   *      functions.col("date").gt("2018-01-01"),
   *      new HashMap<String, Column>() {{
   *        put("data", functions.col("data").plus(1));
   *      }}
   *    );
   * }}}
   *
   * @param condition boolean expression as Column object specifying which rows to update.
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 0.3.0
   */
  def update(condition: Column, set: java.util.Map[String, Column]): Unit

  /**
   * Update rows in the table based on the rules defined by `set`.
   *
   * Scala example to increment the column `data`:
   * {{{
   *    deltaTable.updateExpr(Map("data" -> "data + 1")))
   * }}}
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(set: Map[String, String]): Unit

  /**
   * Update rows in the table based on the rules defined by `set`.
   *
   * Java example to increment the column `data`:
   * {{{
   *    deltaTable.updateExpr(
   *      new HashMap<String, String>() {{
   *        put("data", "data + 1");
   *      }}
   *    );
   * }}}
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(set: java.util.Map[String, String]): Unit

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   *
   * Scala example to increment the column `data`:
   * {{{
   *    deltaTable.update(
   *      "date > '2018-01-01'",
   *      Map("data" -> "data + 1"))
   * }}}
   *
   * @param condition boolean expression as SQL formatted string object specifying
   *                  which rows to update.
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(condition: String, set: Map[String, String]): Unit

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   *
   * Java example to increment the column `data`:
   * {{{
   *    deltaTable.update(
   *      "date > '2018-01-01'",
   *      new HashMap<String, String>() {{
   *        put("data", "data + 1");
   *      }}
   *    );
   * }}}
   *
   * @param condition boolean expression as SQL formatted string object specifying
   *                  which rows to update.
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(condition: String, set: java.util.Map[String, String]): Unit

  /**
   * Merge data from the `source` DataFrame based on the given merge `condition`. This returns
   * a [[DeltaMergeBuilder]] object that can be used to specify the update, delete, or insert
   * actions to be performed on rows based on whether the rows matched the condition or not.
   *
   * See the [[DeltaMergeBuilder]] for a full description of this operation and what combinations
   * of update, delete and insert operations are allowed.
   *
   * Scala example to update a key-value Delta table with new key-values from a source DataFrame:
   * {{{
   *    deltaTable
   *     .as("target")
   *     .merge(
   *       source.as("source"),
   *       "target.key = source.key")
   *     .whenMatched
   *     .updateExpr(Map(
   *       "value" -> "source.value"))
   *     .whenNotMatched
   *     .insertExpr(Map(
   *       "key" -> "source.key",
   *       "value" -> "source.value"))
   *     .execute()
   * }}}
   *
   * @param source source DataFrame to be merged.
   * @param condition boolean expression as SQL formatted string
   * @return DeltaMergeBuilder
   * @since 0.3.0
   */
  def merge(source: DataFrame, condition: String): DeltaMergeBuilder

  /**
   * Merge data from the `source` DataFrame based on the given merge `condition`. This returns
   * a [[DeltaMergeBuilder]] object that can be used to specify the update, delete, or insert
   * actions to be performed on rows based on whether the rows matched the condition or not.
   *
   * See the [[DeltaMergeBuilder]] for a full description of this operation and what combinations
   * of update, delete and insert operations are allowed.
   *
   * @param source source DataFrame to be merged.
   * @param condition boolean expression as a Column object
   * @return DeltaMergeBuilder
   * @since 0.3.0
   */
  def merge(source: DataFrame, condition: Column): DeltaMergeBuilder

  /**
   * Optimize the data layout of the table. This returns
   * a [[DeltaOptimizeBuilder]] object that can be used to specify
   * the partition filter to limit the scope of optimize and
   * also execute different optimization techniques such as file
   * compaction or order data using Z-Order curves.
   *
   * See the [[DeltaOptimizeBuilder]] for a full description
   * of this operation.
   *
   * Scala example to run file compaction on a subset of
   * partitions in the table:
   * {{{
   *    deltaTable
   *     .optimize()
   *     .where("date='2021-11-18'")
   *     .executeCompaction();
   * }}}
   *
   * @return DeltaOptimizeBuilder
   * @since 2.0.0
   */
  def optimize(): DeltaOptimizeBuilder

  /**
   * Restore the DeltaTable to an older version of the table specified by version number.
   *
   * An example would be:
   * {{{
   *   io.delta.tables.DeltaTable.restoreToVersion(7)
   * }}}
   *
   * @param version the version number to restore to
   * @return DataFrame containing restore statistics
   * @since 1.2.0
   */
  def restoreToVersion(version: Long): DataFrame

  /**
   * Restore the DeltaTable to an older version of the table specified by a timestamp.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss
   *
   * An example would be:
   * {{{
   *   io.delta.tables.DeltaTable.restoreToTimestamp("2019-01-01")
   * }}}
   *
   * @param timestamp the timestamp to restore to
   * @return DataFrame containing restore statistics
   * @since 1.2.0
   */
  def restoreToTimestamp(timestamp: String): DataFrame

  /**
   * Updates the protocol version of the table to leverage new features. Upgrading the reader
   * version will prevent all clients that have an older version of Delta Lake from accessing this
   * table. Upgrading the writer version will prevent older versions of Delta Lake to write to
   * this table. The reader or writer version cannot be downgraded.
   *
   * See online documentation and Delta's protocol specification at PROTOCOL.md for more details.
   *
   * @param readerVersion the reader version to upgrade to
   * @param writerVersion the writer version to upgrade to
   * @since 0.8.0
   */
  def upgradeTableProtocol(readerVersion: Int, writerVersion: Int): Unit

  /**
   * Modify the protocol to add a supported feature, and if the table does not support table
   * features, upgrade the protocol automatically. In such a case when the provided feature is
   * writer-only, the table's writer version will be upgraded to `7`, and when the provided
   * feature is reader-writer, both reader and writer versions will be upgraded, to `(3, 7)`.
   *
   * See online documentation and Delta's protocol specification at PROTOCOL.md for more details.
   *
   * @param featureName the name of the feature to add
   * @since 2.3.0
   */
  def addFeatureSupport(featureName: String): Unit

  /**
   * Modify the protocol to drop a supported feature. The operation always normalizes the
   * resulting protocol.
   *
   * See online documentation for more details.
   *
   * @param featureName The name of the feature to drop.
   * @param truncateHistory Whether to truncate history before downgrading the protocol.
   * @since 3.4.0
   */
  def dropFeatureSupport(featureName: String, truncateHistory: Boolean): Unit

  /**
   * Modify the protocol to drop a supported feature. The operation always normalizes the
   * resulting protocol.
   *
   * Note, this command will not truncate history.
   *
   * See online documentation for more details.
   *
   * @param featureName The name of the feature to drop.
   * @since 3.4.0
   */
  def dropFeatureSupport(featureName: String): Unit

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   *
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @param replace Whether to replace the destination with the clone command.
   * @param properties The table properties to override in the clone.
   * @return The cloned DeltaTable
   * @since 3.3.0
   */
  def clone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   *
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @param replace Whether to replace the destination with the clone command.
   * @param properties The table properties to override in the clone (Java Map).
   * @return The cloned DeltaTable
   */
  def clone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   *
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @param replace Whether to replace the destination with the clone command.
   * @return The cloned DeltaTable
   * @since 3.3.0
   */
  def clone(target: String, isShallow: Boolean, replace: Boolean): DeltaTable

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   *
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @return The cloned DeltaTable
   * @since 3.3.0
   */
  def clone(target: String, isShallow: Boolean): DeltaTable

  /**
   * Clone a DeltaTable at a specific version to a given destination.
   *
   * @param version The version of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @param replace Whether to replace the destination with the clone command.
   * @param properties The table properties to override in the clone.
   * @return The cloned DeltaTable
   * @since 3.3.0
   */
  def cloneAtVersion(
      version: Long,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable

  /**
   * Clone a DeltaTable at a specific version to a given destination.
   *
   * @param version The version of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @param replace Whether to replace the destination with the clone command.
   * @param properties The table properties to override in the clone (Java Map).
   * @return The cloned DeltaTable
   */
  def cloneAtVersion(
      version: Long,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable

  /**
   * Clone a DeltaTable at a specific version to a given destination.
   *
   * @param version The version of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @param replace Whether to replace the destination with the clone command.
   * @return The cloned DeltaTable
   * @since 3.3.0
   */
  def cloneAtVersion(
      version: Long,
      target: String,
      isShallow: Boolean,
      replace: Boolean): DeltaTable

  /**
   * Clone a DeltaTable at a specific version to a given destination.
   *
   * @param version The version of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @return The cloned DeltaTable
   * @since 3.3.0
   */
  def cloneAtVersion(version: Long, target: String, isShallow: Boolean): DeltaTable

  /**
   * Clone a DeltaTable at a specific timestamp to a given destination.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.
   *
   * @param timestamp The timestamp of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @param replace Whether to replace the destination with the clone command.
   * @param properties The table properties to override in the clone.
   * @return The cloned DeltaTable
   * @since 3.3.0
   */
  def cloneAtTimestamp(
      timestamp: String,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable

  /**
   * Clone a DeltaTable at a specific timestamp to a given destination.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.
   *
   * @param timestamp The timestamp of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @param replace Whether to replace the destination with the clone command.
   * @param properties The table properties to override in the clone (Java Map).
   * @return The cloned DeltaTable
   */
  def cloneAtTimestamp(
      timestamp: String,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable

  /**
   * Clone a DeltaTable at a specific timestamp to a given destination.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.
   *
   * @param timestamp The timestamp of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @param replace Whether to replace the destination with the clone command.
   * @return The cloned DeltaTable
   * @since 3.3.0
   */
  def cloneAtTimestamp(
      timestamp: String,
      target: String,
      isShallow: Boolean,
      replace: Boolean): DeltaTable

  /**
   * Clone a DeltaTable at a specific timestamp to a given destination.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.
   *
   * @param timestamp The timestamp of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param isShallow Whether to create a shallow clone or a deep clone.
   * @return The cloned DeltaTable
   * @since 3.3.0
   */
  def cloneAtTimestamp(timestamp: String, target: String, isShallow: Boolean): DeltaTable
}
