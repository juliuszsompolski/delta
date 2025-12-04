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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaTableUtils.withActiveSession
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.catalog.{CatalogResolver, DeltaTableV2}
import org.apache.spark.sql.delta.commands.{
  AlterTableDropFeatureDeltaCommand,
  AlterTableSetPropertiesDeltaCommand
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.tables.{
  DeltaColumnBuilder,
  DeltaMergeBuilder,
  DeltaOptimizeBuilder,
  DeltaTableBuilder,
  DeltaTableCompanion,
  CreateTableOptions,
  ReplaceTableOptions
}
import io.delta.tables.classic.execution._
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

/**
 * Classic (local Spark) implementation of DeltaTable.
 */
class DeltaTable private[tables](
    @transient private val _df: Dataset[Row],
    @transient private val table: DeltaTableV2)
  extends io.delta.tables.DeltaTable
  with DeltaTableOperations
  with Serializable {

  protected def deltaLog: DeltaLog = {
    /** Assert the codes run in the driver. */
    if (table == null) {
      throw DeltaErrors.deltaTableFoundInExecutor()
    }

    table.deltaLog
  }

  protected def df: Dataset[Row] = {
    /** Assert the codes run in the driver. */
    if (_df == null) {
      throw DeltaErrors.deltaTableFoundInExecutor()
    }

    _df
  }

  /** @inheritdoc */
  override def as(alias: String): DeltaTable = new DeltaTable(df.as(alias), table)

  /** @inheritdoc */
  override def alias(alias: String): DeltaTable = as(alias)

  /** @inheritdoc */
  override def toDF: Dataset[Row] = df

  /** @inheritdoc */
  override def vacuum(retentionHours: Double): DataFrame = {
    executeVacuum(table, Some(retentionHours))
  }

  /** @inheritdoc */
  override def vacuum(): DataFrame = {
    executeVacuum(table, retentionHours = None)
  }

  /** @inheritdoc */
  override def history(limit: Int): DataFrame = {
    executeHistory(deltaLog, Some(limit), table.catalogTable)
  }

  /** @inheritdoc */
  override def history(): DataFrame = {
    executeHistory(deltaLog, catalogTable = table.catalogTable)
  }

  /** @inheritdoc */
  @Evolving
  override def detail(): DataFrame = {
    executeDetails(deltaLog.dataPath.toString, table.getTableIdentifierIfExists)
  }

  /** @inheritdoc */
  override def generate(mode: String): Unit = {
    executeGenerate(deltaLog.dataPath.toString, table.getTableIdentifierIfExists, mode)
  }

  /** @inheritdoc */
  override def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  /** @inheritdoc */
  override def delete(condition: Column): Unit = {
    executeDelete(Some(condition.expr))
  }

  /** @inheritdoc */
  override def delete(): Unit = {
    executeDelete(None)
  }

  /** @inheritdoc */
  override def optimize(): DeltaOptimizeBuilder =
    new io.delta.tables.classic.DeltaOptimizeBuilder(table)

  /** @inheritdoc */
  override def update(set: Map[String, Column]): Unit = {
    executeUpdate(set, None)
  }

  /** @inheritdoc */
  override def update(set: java.util.Map[String, Column]): Unit = {
    executeUpdate(set.asScala, None)
  }

  /** @inheritdoc */
  override def update(condition: Column, set: Map[String, Column]): Unit = {
    executeUpdate(set, Some(condition))
  }

  /** @inheritdoc */
  override def update(condition: Column, set: java.util.Map[String, Column]): Unit = {
    executeUpdate(set.asScala, Some(condition))
  }

  /** @inheritdoc */
  override def updateExpr(set: Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set), None)
  }

  /** @inheritdoc */
  override def updateExpr(set: java.util.Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set.asScala), None)
  }

  /** @inheritdoc */
  override def updateExpr(condition: String, set: Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set), Some(functions.expr(condition)))
  }

  /** @inheritdoc */
  override def updateExpr(condition: String, set: java.util.Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set.asScala), Some(functions.expr(condition)))
  }

  /** @inheritdoc */
  override def merge(source: DataFrame, condition: String): DeltaMergeBuilder = {
    merge(source, functions.expr(condition))
  }

  /** @inheritdoc */
  override def merge(source: DataFrame, condition: Column): DeltaMergeBuilder = {
    io.delta.tables.classic.DeltaMergeBuilder(this, source, condition)
  }

  /** @inheritdoc */
  override def restoreToVersion(version: Long): DataFrame = {
    executeRestore(table, Some(version), None)
  }

  /** @inheritdoc */
  override def restoreToTimestamp(timestamp: String): DataFrame = {
    executeRestore(table, None, Some(timestamp))
  }

  /** @inheritdoc */
  override def upgradeTableProtocol(readerVersion: Int, writerVersion: Int): Unit =
    withActiveSession(sparkSession) {
      val alterTableCmd = AlterTableSetPropertiesDeltaCommand(
        table,
        DeltaConfigs.validateConfigurations(
          Map(
            "delta.minReaderVersion" -> readerVersion.toString,
            "delta.minWriterVersion" -> writerVersion.toString)))
      toDataset(sparkSession, alterTableCmd)
    }

  /** @inheritdoc */
  override def addFeatureSupport(featureName: String): Unit = withActiveSession(sparkSession) {
    val alterTableCmd = AlterTableSetPropertiesDeltaCommand(
      table,
      Map(
        TableFeatureProtocolUtils.propertyKey(featureName) ->
          TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED))
    toDataset(sparkSession, alterTableCmd)
  }

  private def executeDropFeature(featureName: String, truncateHistory: Option[Boolean]): Unit = {
    val alterTableCmd = AlterTableDropFeatureDeltaCommand(
      table = table,
      featureName = featureName,
      truncateHistory = truncateHistory.getOrElse(false))
    toDataset(sparkSession, alterTableCmd)
  }

  /** @inheritdoc */
  override def dropFeatureSupport(
      featureName: String,
      truncateHistory: Boolean): Unit = withActiveSession(sparkSession) {
    executeDropFeature(featureName, Some(truncateHistory))
  }

  /** @inheritdoc */
  override def dropFeatureSupport(featureName: String): Unit = withActiveSession(sparkSession) {
    executeDropFeature(featureName, None)
  }

  /** @inheritdoc */
  override def clone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable = {
    executeClone(
      table,
      target,
      isShallow,
      replace,
      properties,
      versionAsOf = None,
      timestampAsOf = None)
  }

  /** @inheritdoc */
  override def clone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable = {
    val scalaProps = Option(properties).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    executeClone(
      table,
      target,
      isShallow,
      replace,
      scalaProps,
      versionAsOf = None,
      timestampAsOf = None)
  }

  /** @inheritdoc */
  override def clone(target: String, isShallow: Boolean, replace: Boolean): DeltaTable = {
    clone(target, isShallow, replace, properties = Map.empty[String, String])
  }

  /** @inheritdoc */
  override def clone(target: String, isShallow: Boolean): DeltaTable = {
    clone(target, isShallow, replace = false)
  }

  /** @inheritdoc */
  override def cloneAtVersion(
      version: Long,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable = {
    executeClone(
      table,
      target,
      isShallow,
      replace,
      properties,
      versionAsOf = Some(version),
      timestampAsOf = None)
  }

  /** @inheritdoc */
  override def cloneAtVersion(
      version: Long,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable = {
    val scalaProps = Option(properties).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    executeClone(
      table,
      target,
      isShallow,
      replace,
      scalaProps,
      versionAsOf = Some(version),
      timestampAsOf = None)
  }

  /** @inheritdoc */
  override def cloneAtVersion(
      version: Long,
      target: String,
      isShallow: Boolean,
      replace: Boolean): DeltaTable = {
    cloneAtVersion(version, target, isShallow, replace, properties = Map.empty[String, String])
  }

  /** @inheritdoc */
  override def cloneAtVersion(version: Long, target: String, isShallow: Boolean): DeltaTable = {
    cloneAtVersion(version, target, isShallow, replace = false)
  }

  /** @inheritdoc */
  override def cloneAtTimestamp(
      timestamp: String,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable = {
    executeClone(
      table,
      target,
      isShallow,
      replace,
      properties,
      versionAsOf = None,
      timestampAsOf = Some(timestamp))
  }

  /** @inheritdoc */
  override def cloneAtTimestamp(
      timestamp: String,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable = {
    val scalaProps = Option(properties).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    executeClone(
      table,
      target,
      isShallow,
      replace,
      scalaProps,
      versionAsOf = None,
      timestampAsOf = Some(timestamp))
  }

  /** @inheritdoc */
  override def cloneAtTimestamp(
      timestamp: String,
      target: String,
      isShallow: Boolean,
      replace: Boolean): DeltaTable = {
    cloneAtTimestamp(timestamp, target, isShallow, replace, properties = Map.empty[String, String])
  }

  /** @inheritdoc */
  override def cloneAtTimestamp(
      timestamp: String,
      target: String,
      isShallow: Boolean): DeltaTable = {
    cloneAtTimestamp(timestamp, target, isShallow, replace = false)
  }
}

/**
 * Classic implementation companion object with factory methods.
 */
object DeltaTable extends DeltaTableCompanion {

  /** @inheritdoc */
  override def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: StructType): DeltaTable = {
    val tableId: TableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(identifier)
    DeltaConvert.executeConvert(spark, tableId, Some(partitionSchema), None)
  }

  /** @inheritdoc */
  override def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: String): DeltaTable = {
    val tableId: TableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(identifier)
    DeltaConvert.executeConvert(spark, tableId, Some(StructType.fromDDL(partitionSchema)), None)
  }

  /** @inheritdoc */
  override def convertToDelta(
      spark: SparkSession,
      identifier: String): DeltaTable = {
    val tableId: TableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(identifier)
    DeltaConvert.executeConvert(spark, tableId, None, None)
  }

  /** @inheritdoc */
  override def forPath(path: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    forPath(sparkSession, path)
  }

  /** @inheritdoc */
  override def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    forPath(sparkSession, path, Map.empty[String, String])
  }

  /** @inheritdoc */
  override def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: scala.collection.Map[String, String]): DeltaTable = {
    val badOptions = hadoopConf.filterKeys { k =>
      !DeltaTableUtils.validDeltaTableHadoopPrefixes.exists(k.startsWith)
    }.toMap
    if (!badOptions.isEmpty) {
      throw DeltaErrors.unsupportedDeltaTableForPathHadoopConf(badOptions)
    }
    val fileSystemOptions: Map[String, String] = hadoopConf.toMap
    val hdpPath = new Path(path)
    if (DeltaTableUtils.isDeltaTable(sparkSession, hdpPath, fileSystemOptions)) {
      new DeltaTable(sparkSession.read.format("delta").options(fileSystemOptions).load(path),
        DeltaTableV2(
          spark = sparkSession,
          path = hdpPath,
          options = fileSystemOptions))
    } else {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(path = Some(path)))
    }
  }

  /** @inheritdoc */
  override def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: java.util.Map[String, String]): DeltaTable = {
    val fsOptions = hadoopConf.asScala.toMap
    forPath(sparkSession, path, fsOptions)
  }

  /** @inheritdoc */
  override def forName(tableOrViewName: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    forName(sparkSession, tableOrViewName)
  }

  private def getDeltaTableFromSessionCatalog(
      spark: SparkSession,
      tableName: String): DeltaTable = {
    val tableId = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    if (DeltaTableUtils.isDeltaTable(spark, tableId)) {
      val tbl = spark.sessionState.catalog.getTableMetadata(tableId)
      new DeltaTable(
        spark.table(tableName),
        DeltaTableV2(spark, new Path(tbl.location), Some(tbl), Some(tableName)))
    } else if (DeltaTableUtils.isValidPath(tableId)) {
      forPath(spark, tableId.table)
    } else {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(table = Some(tableId)))
    }
  }

  /** @inheritdoc */
  override def forName(sparkSession: SparkSession, tableName: String): DeltaTable = {
    sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName) match {
      case parts if parts.length == 3 =>
        val (catalog, ident) =
          CatalogResolver.getCatalogPluginAndIdentifier(sparkSession, parts.head, parts.tail)
        new DeltaTable(
          sparkSession.table(tableName),
          CatalogResolver.getDeltaTableFromCatalog(sparkSession, catalog, ident)
        )
      case _ =>
        getDeltaTableFromSessionCatalog(sparkSession, tableName)
    }
  }

  /** @inheritdoc */
  override def isDeltaTable(sparkSession: SparkSession, identifier: String): Boolean = {
    val identifierPath = new Path(identifier)
    if (sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_STRICT_CHECK_DELTA_TABLE)) {
      val rootOption = DeltaTableUtils.findDeltaTableRoot(sparkSession, identifierPath)
      rootOption.isDefined && DeltaLog.forTable(sparkSession, rootOption.get).tableExists
    } else {
      DeltaTableUtils.isDeltaTable(sparkSession, identifierPath)
    }
  }

  /** @inheritdoc */
  override def isDeltaTable(identifier: String): Boolean = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    isDeltaTable(sparkSession, identifier)
  }

  /** @inheritdoc */
  @Evolving
  override def create(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    create(sparkSession)
  }

  /** @inheritdoc */
  @Evolving
  override def create(spark: SparkSession): DeltaTableBuilder = {
    new io.delta.tables.classic.DeltaTableBuilder(spark, CreateTableOptions(ifNotExists = false))
  }

  /** @inheritdoc */
  @Evolving
  override def createIfNotExists(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    createIfNotExists(sparkSession)
  }

  /** @inheritdoc */
  @Evolving
  override def createIfNotExists(spark: SparkSession): DeltaTableBuilder = {
    new io.delta.tables.classic.DeltaTableBuilder(spark, CreateTableOptions(ifNotExists = true))
  }

  /** @inheritdoc */
  @Evolving
  override def replace(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    replace(sparkSession)
  }

  /** @inheritdoc */
  @Evolving
  override def replace(spark: SparkSession): DeltaTableBuilder = {
    new io.delta.tables.classic.DeltaTableBuilder(spark, ReplaceTableOptions(orCreate = false))
  }

  /** @inheritdoc */
  @Evolving
  override def createOrReplace(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    createOrReplace(sparkSession)
  }

  /** @inheritdoc */
  @Evolving
  override def createOrReplace(spark: SparkSession): DeltaTableBuilder = {
    new io.delta.tables.classic.DeltaTableBuilder(spark, ReplaceTableOptions(orCreate = true))
  }

  /** @inheritdoc */
  @Evolving
  override def columnBuilder(colName: String): DeltaColumnBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    columnBuilder(sparkSession, colName)
  }

  /** @inheritdoc */
  @Evolving
  override def columnBuilder(spark: SparkSession, colName: String): DeltaColumnBuilder = {
    new io.delta.tables.classic.DeltaColumnBuilder(spark, colName)
  }
}
