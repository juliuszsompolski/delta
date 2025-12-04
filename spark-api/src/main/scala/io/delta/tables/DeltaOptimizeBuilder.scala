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
import org.apache.spark.sql.DataFrame

/**
 * Builder class for constructing OPTIMIZE command and executing.
 *
 * @since 2.0.0
 */
abstract class DeltaOptimizeBuilder {

  /**
   * Apply partition filter on this optimize command builder to limit
   * the operation on selected partitions.
   *
   * @param partitionFilter The partition filter to apply
   * @return [[DeltaOptimizeBuilder]] with partition filter applied
   * @since 2.0.0
   */
  def where(partitionFilter: String): DeltaOptimizeBuilder

  /**
   * Compact the small files in selected partitions.
   *
   * @return DataFrame containing the OPTIMIZE execution metrics
   * @since 2.0.0
   */
  def executeCompaction(): DataFrame

  /**
   * Z-Order the data in selected partitions using the given columns.
   *
   * @param columns Zero or more columns to order the data using Z-Order curves
   * @return DataFrame containing the OPTIMIZE execution metrics
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def executeZOrderBy(columns: String*): DataFrame
}
