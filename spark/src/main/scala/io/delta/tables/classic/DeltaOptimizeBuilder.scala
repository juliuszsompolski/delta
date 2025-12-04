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

import org.apache.spark.sql.delta.DeltaTableUtils.withActiveSession
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.DeltaOptimizeContext
import org.apache.spark.sql.delta.commands.OptimizeTableCommand
import org.apache.spark.sql.delta.util.AnalysisHelper

import org.apache.spark.annotation._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedAttribute}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

/**
 * Classic (local Spark) implementation of DeltaOptimizeBuilder.
 */
class DeltaOptimizeBuilder private[tables](table: DeltaTableV2)
  extends io.delta.tables.DeltaOptimizeBuilder
  with AnalysisHelper {

  private var partitionFilter: Seq[String] = Seq.empty

  private lazy val tableIdentifier: String =
    table.tableIdentifier.getOrElse(s"delta.`${table.deltaLog.dataPath.toString}`")

  /** @inheritdoc */
  override def where(partitionFilter: String): DeltaOptimizeBuilder = {
    this.partitionFilter = this.partitionFilter :+ partitionFilter
    this
  }

  /** @inheritdoc */
  override def executeCompaction(): DataFrame = {
    execute(Seq.empty)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  override def executeZOrderBy(columns: String*): DataFrame = {
    val attrs = columns.map(c => UnresolvedAttribute(c))
    execute(attrs)
  }

  private def execute(zOrderBy: Seq[UnresolvedAttribute]): DataFrame = {
    val sparkSession = table.spark
    withActiveSession(sparkSession) {
      val tableId: TableIdentifier = sparkSession
        .sessionState
        .sqlParser
        .parseTableIdentifier(tableIdentifier)
      val id = Identifier.of(tableId.database.toArray, tableId.identifier)
      val catalogPlugin = sparkSession.sessionState.catalogManager.currentCatalog
      val catalog = catalogPlugin match {
        case tableCatalog: TableCatalog => tableCatalog
        case _ => throw new IllegalArgumentException(
          s"Catalog ${catalogPlugin.name} does not support tables")
      }
      val resolvedTable = ResolvedTable.create(catalog, id, table)
      val optimize = OptimizeTableCommand(
        resolvedTable, partitionFilter, DeltaOptimizeContext())(zOrderBy = zOrderBy)
      toDataset(sparkSession, optimize)
    }
  }
}

private[tables] object DeltaOptimizeBuilder {
  /**
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[tables] def apply(table: DeltaTableV2): DeltaOptimizeBuilder =
    new DeltaOptimizeBuilder(table)
}
