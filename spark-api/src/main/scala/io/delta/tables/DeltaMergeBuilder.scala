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
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Builder to specify how to merge data from source DataFrame into the target Delta table.
 * You can specify any number of `whenMatched` and `whenNotMatched` clauses.
 * Here are the constraints on these clauses.
 *
 *   - `whenMatched` clauses:
 *
 *     - The condition in a `whenMatched` clause is optional. However, if there are multiple
 *       `whenMatched` clauses, then only the last one may omit the condition.
 *
 *     - When there are more than one `whenMatched` clauses and there are conditions (or the lack
 *       of) such that a row satisfies multiple clauses, then the action for the first clause
 *       satisfied is executed. In other words, the order of the `whenMatched` clauses matters.
 *
 *     - If none of the `whenMatched` clauses match a source-target row pair that satisfy
 *       the merge condition, then the target rows will not be updated or deleted.
 *
 *     - If you want to update all the columns of the target Delta table with the
 *       corresponding column of the source DataFrame, then you can use the
 *       `whenMatched(...).updateAll()`. This is equivalent to
 *       {{{
 *         whenMatched(...).updateExpr(Map(
 *           ("col1", "source.col1"),
 *           ("col2", "source.col2"),
 *           ...))
 *       }}}
 *
 *   - `whenNotMatched` clauses:
 *
 *     - The condition in a `whenNotMatched` clause is optional. However, if there are
 *       multiple `whenNotMatched` clauses, then only the last one may omit the condition.
 *
 *     - When there are more than one `whenNotMatched` clauses and there are conditions (or the
 *       lack of) such that a row satisfies multiple clauses, then the action for the first clause
 *       satisfied is executed. In other words, the order of the `whenNotMatched` clauses matters.
 *
 *     - If no `whenNotMatched` clause is present or if it is present but the non-matching source
 *       row does not satisfy the condition, then the source row is not inserted.
 *
 *     - If you want to insert all the columns of the target Delta table with the
 *       corresponding column of the source DataFrame, then you can use
 *       `whenNotMatched(...).insertAll()`. This is equivalent to
 *       {{{
 *         whenNotMatched(...).insertExpr(Map(
 *           ("col1", "source.col1"),
 *           ("col2", "source.col2"),
 *           ...))
 *       }}}
 *
 *   - `whenNotMatchedBySource` clauses:
 *
 *     - The condition in a `whenNotMatchedBySource` clause is optional. However, if there are
 *       multiple `whenNotMatchedBySource` clauses, then only the last one may omit the condition.
 *
 *     - When there are more than one `whenNotMatchedBySource` clauses and there are conditions (or
 *       the lack of) such that a row satisfies multiple clauses, then the action for the first
 *       clause satisfied is executed. In other words, the order of the `whenNotMatchedBySource`
 *       clauses matters.
 *
 *     - If no `whenNotMatchedBySource` clause is present or if it is present but the
 *       non-matching target row does not satisfy any of the `whenNotMatchedBySource` clause
 *       condition, then the target row will not be updated or deleted.
 *
 *
 * Scala example to update a key-value Delta table with new key-values from a source DataFrame:
 * {{{
 *    deltaTable
 *     .as("target")
 *     .merge(
 *       source.as("source"),
 *       "target.key = source.key")
 *     .withSchemaEvolution()
 *     .whenMatched()
 *     .updateExpr(Map(
 *       "value" -> "source.value"))
 *     .whenNotMatched()
 *     .insertExpr(Map(
 *       "key" -> "source.key",
 *       "value" -> "source.value"))
 *     .whenNotMatchedBySource()
 *     .updateExpr(Map(
 *       "value" -> "target.value + 1"))
 *     .execute()
 * }}}
 *
 * Java example to update a key-value Delta table with new key-values from a source DataFrame:
 * {{{
 *    deltaTable
 *     .as("target")
 *     .merge(
 *       source.as("source"),
 *       "target.key = source.key")
 *     .withSchemaEvolution()
 *     .whenMatched()
 *     .updateExpr(
 *        new HashMap<String, String>() {{
 *          put("value", "source.value");
 *        }})
 *     .whenNotMatched()
 *     .insertExpr(
 *        new HashMap<String, String>() {{
 *         put("key", "source.key");
 *         put("value", "source.value");
 *       }})
 *     .whenNotMatchedBySource()
 *     .updateExpr(
 *        new HashMap<String, String>() {{
 *         put("value", "target.value + 1");
 *       }})
 *     .execute();
 * }}}
 *
 * @since 0.3.0
 */
abstract class DeltaMergeBuilder {

  /**
   * Build the actions to perform when the merge condition was matched. This returns
   * [[DeltaMergeMatchedActionBuilder]] object which can be used to specify how
   * to update or delete the matched target table row with the source row.
   *
   * @return DeltaMergeMatchedActionBuilder
   * @since 0.3.0
   */
  def whenMatched(): DeltaMergeMatchedActionBuilder

  /**
   * Build the actions to perform when the merge condition was matched and
   * the given `condition` is true. This returns [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to update or delete the matched target table row with the
   * source row.
   *
   * @param condition boolean expression as a SQL formatted string
   * @return DeltaMergeMatchedActionBuilder
   * @since 0.3.0
   */
  def whenMatched(condition: String): DeltaMergeMatchedActionBuilder

  /**
   * Build the actions to perform when the merge condition was matched and
   * the given `condition` is true. This returns a [[DeltaMergeMatchedActionBuilder]] object
   * which can be used to specify how to update or delete the matched target table row with the
   * source row.
   *
   * @param condition boolean expression as a Column object
   * @return DeltaMergeMatchedActionBuilder
   * @since 0.3.0
   */
  def whenMatched(condition: Column): DeltaMergeMatchedActionBuilder

  /**
   * Build the action to perform when the merge condition was not matched. This returns
   * [[DeltaMergeNotMatchedActionBuilder]] object which can be used to specify how
   * to insert the new sourced row into the target table.
   *
   * @return DeltaMergeNotMatchedActionBuilder
   * @since 0.3.0
   */
  def whenNotMatched(): DeltaMergeNotMatchedActionBuilder

  /**
   * Build the actions to perform when the merge condition was not matched and
   * the given `condition` is true. This returns [[DeltaMergeNotMatchedActionBuilder]] object
   * which can be used to specify how to insert the new sourced row into the target table.
   *
   * @param condition boolean expression as a SQL formatted string
   * @return DeltaMergeNotMatchedActionBuilder
   * @since 0.3.0
   */
  def whenNotMatched(condition: String): DeltaMergeNotMatchedActionBuilder

  /**
   * Build the actions to perform when the merge condition was not matched and
   * the given `condition` is true. This returns [[DeltaMergeNotMatchedActionBuilder]] object
   * which can be used to specify how to insert the new sourced row into the target table.
   *
   * @param condition boolean expression as a Column object
   * @return DeltaMergeNotMatchedActionBuilder
   * @since 0.3.0
   */
  def whenNotMatched(condition: Column): DeltaMergeNotMatchedActionBuilder

  /**
   * Build the actions to perform when the merge condition was not matched by the source. This
   * returns [[DeltaMergeNotMatchedBySourceActionBuilder]] object which can be used to specify how
   * to update or delete the target table row.
   *
   * @return DeltaMergeNotMatchedBySourceActionBuilder
   * @since 2.3.0
   */
  def whenNotMatchedBySource(): DeltaMergeNotMatchedBySourceActionBuilder

  /**
   * Build the actions to perform when the merge condition was not matched by the source and the
   * given `condition` is true. This returns [[DeltaMergeNotMatchedBySourceActionBuilder]] object
   * which can be used to specify how to update or delete the target table row.
   *
   * @param condition boolean expression as a SQL formatted string
   * @return DeltaMergeNotMatchedBySourceActionBuilder
   * @since 2.3.0
   */
  def whenNotMatchedBySource(condition: String): DeltaMergeNotMatchedBySourceActionBuilder

  /**
   * Build the actions to perform when the merge condition was not matched by the source and the
   * given `condition` is true. This returns [[DeltaMergeNotMatchedBySourceActionBuilder]] object
   * which can be used to specify how to update or delete the target table row.
   *
   * @param condition boolean expression as a Column object
   * @return DeltaMergeNotMatchedBySourceActionBuilder
   * @since 2.3.0
   */
  def whenNotMatchedBySource(condition: Column): DeltaMergeNotMatchedBySourceActionBuilder

  /**
   * Enable schema evolution for the merge operation. This allows the schema of the target
   * table/columns to be automatically updated based on the schema of the source table/columns.
   *
   * @return this DeltaMergeBuilder with schema evolution enabled
   * @since 3.2.0
   */
  def withSchemaEvolution(): DeltaMergeBuilder

  /**
   * Execute the merge operation based on the built matched and not matched actions.
   *
   * @return DataFrame containing merge operation metrics
   * @since 0.3.0
   */
  def execute(): DataFrame
}

/**
 * Builder class to specify the actions to perform when a target table row has matched a
 * source row based on the given merge condition and optional match condition.
 *
 * See [[DeltaMergeBuilder]] for more information.
 *
 * @since 0.3.0
 */
abstract class DeltaMergeMatchedActionBuilder {

  /**
   * Update the matched table rows based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def update(set: Map[String, Column]): DeltaMergeBuilder

  /**
   * Update the matched table rows based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def updateExpr(set: Map[String, String]): DeltaMergeBuilder

  /**
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as Column objects.
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def update(set: java.util.Map[String, Column]): DeltaMergeBuilder

  /**
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as SQL formatted strings.
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def updateExpr(set: java.util.Map[String, String]): DeltaMergeBuilder

  /**
   * Update all the columns of the matched table row with the values of the
   * corresponding columns in the source row.
   *
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def updateAll(): DeltaMergeBuilder

  /**
   * Delete a matched row from the table.
   *
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def delete(): DeltaMergeBuilder
}

/**
 * Builder class to specify the actions to perform when a source row has not matched any target
 * Delta table row based on the merge condition, but has matched the additional condition
 * if specified.
 *
 * See [[DeltaMergeBuilder]] for more information.
 *
 * @since 0.3.0
 */
abstract class DeltaMergeNotMatchedActionBuilder {

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as Column objects.
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def insert(values: Map[String, Column]): DeltaMergeBuilder

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as SQL formatted strings.
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def insertExpr(values: Map[String, String]): DeltaMergeBuilder

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as Column objects.
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def insert(values: java.util.Map[String, Column]): DeltaMergeBuilder

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as SQL formatted strings.
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def insertExpr(values: java.util.Map[String, String]): DeltaMergeBuilder

  /**
   * Insert a new target Delta table row by assigning the target columns to the values of the
   * corresponding columns in the source row.
   *
   * @return the parent DeltaMergeBuilder
   * @since 0.3.0
   */
  def insertAll(): DeltaMergeBuilder
}

/**
 * Builder class to specify the actions to perform when a target table row has not matched any
 * rows in the source table based on the merge condition, but has matched the additional condition
 * if specified.
 *
 * See [[DeltaMergeBuilder]] for more information.
 *
 * @since 2.3.0
 */
abstract class DeltaMergeNotMatchedBySourceActionBuilder {

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @return the parent DeltaMergeBuilder
   * @since 2.3.0
   */
  def update(set: Map[String, Column]): DeltaMergeBuilder

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @return the parent DeltaMergeBuilder
   * @since 2.3.0
   */
  def updateExpr(set: Map[String, String]): DeltaMergeBuilder

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as Column objects.
   * @return the parent DeltaMergeBuilder
   * @since 2.3.0
   */
  def update(set: java.util.Map[String, Column]): DeltaMergeBuilder

  /**
   * Update an unmatched target table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as SQL formatted strings.
   * @return the parent DeltaMergeBuilder
   * @since 2.3.0
   */
  def updateExpr(set: java.util.Map[String, String]): DeltaMergeBuilder

  /**
   * Delete an unmatched row from the target table.
   *
   * @return the parent DeltaMergeBuilder
   * @since 2.3.0
   */
  def delete(): DeltaMergeBuilder
}
