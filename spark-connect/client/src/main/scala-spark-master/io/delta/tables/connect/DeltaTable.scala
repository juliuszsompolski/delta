/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.tables.connect

import scala.collection.JavaConverters._

import io.delta.connect.proto
import io.delta.connect.spark.{proto => spark_proto}
import io.delta.tables.{
  DeltaColumnBuilder,
  DeltaMergeBuilder,
  DeltaOptimizeBuilder,
  DeltaTableBuilder,
  DeltaTableCompanion,
  CreateTableOptions,
  ReplaceTableOptions
}

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.{functions, AnalysisException, Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.PrimitiveBooleanEncoder
import org.apache.spark.sql.connect.ColumnNodeToProtoConverter.toExpr
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._
import org.apache.spark.sql.types.StructType

/**
 * Connect (remote Spark) implementation of DeltaTable.
 */
class DeltaTable private[tables](
    private val df: Dataset[Row],
    private val table: proto.DeltaTable)
  extends io.delta.tables.DeltaTable
  with Serializable {

  private def sparkSession: SparkSession = df.sparkSession

  /** @inheritdoc */
  override def as(alias: String): DeltaTable = new DeltaTable(df.as(alias), table)

  /** @inheritdoc */
  override def alias(alias: String): DeltaTable = as(alias)

  /** @inheritdoc */
  override def toDF: Dataset[Row] = df

  private def executeVacuum(retentionHours: Option[Double]): DataFrame = {
    val vacuum = proto.VacuumTable
      .newBuilder()
      .setTable(table)
    retentionHours.foreach(vacuum.setRetentionHours)
    val command = proto.DeltaCommand
      .newBuilder()
      .setVacuumTable(vacuum)
      .build()
    execute(command)
    sparkSession.emptyDataFrame
  }

  /** @inheritdoc */
  override def vacuum(retentionHours: Double): DataFrame = {
    executeVacuum(Some(retentionHours))
  }

  /** @inheritdoc */
  override def vacuum(): DataFrame = {
    executeVacuum(None)
  }

  private def executeHistory(limit: Option[Int]): DataFrame = {
    val describeHistory = proto.DescribeHistory
      .newBuilder()
      .setTable(table)
    val relation = proto.DeltaRelation.newBuilder().setDescribeHistory(describeHistory).build()
    val extension = com.google.protobuf.Any.pack(relation)
    val sparkRelation = spark_proto.Relation.newBuilder().setExtension(extension).build()
    val resultDf = sparkSession.newDataFrame(_.mergeFrom(sparkRelation))
    limit match {
      case Some(l) => resultDf.limit(l)
      case None => resultDf
    }
  }

  /** @inheritdoc */
  override def history(limit: Int): DataFrame = {
    executeHistory(Some(limit))
  }

  /** @inheritdoc */
  override def history(): DataFrame = {
    executeHistory(limit = None)
  }

  /** @inheritdoc */
  @Evolving
  override def detail(): DataFrame = {
    val describeDetail = proto.DescribeDetail
      .newBuilder()
      .setTable(table)
    val relation = proto.DeltaRelation.newBuilder().setDescribeDetail(describeDetail).build()
    val extension = com.google.protobuf.Any.pack(relation)
    val sparkRelation = spark_proto.Relation.newBuilder().setExtension(extension).build()
    sparkSession.newDataFrame(_.mergeFrom(sparkRelation))
  }

  /** @inheritdoc */
  override def generate(mode: String): Unit = {
    val generate = proto.Generate
      .newBuilder()
      .setTable(table)
      .setMode(mode)
    val command = proto.DeltaCommand.newBuilder().setGenerate(generate).build()
    execute(command)
  }

  private def executeDelete(condition: Option[Column]): Unit = {
    val delete = proto.DeleteFromTable
      .newBuilder()
      .setTarget(df.plan.getRoot)
    condition.foreach(c => delete.setCondition(toExpr(c)))
    val relation = proto.DeltaRelation.newBuilder().setDeleteFromTable(delete).build()
    val extension = com.google.protobuf.Any.pack(relation)
    val sparkRelation = spark_proto.Relation.newBuilder().setExtension(extension).build()
    sparkSession.newDataFrame(_.mergeFrom(sparkRelation)).collect()
  }

  /** @inheritdoc */
  override def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  /** @inheritdoc */
  override def delete(condition: Column): Unit = {
    executeDelete(condition = Some(condition))
  }

  /** @inheritdoc */
  override def delete(): Unit = {
    executeDelete(condition = None)
  }

  /** @inheritdoc */
  override def optimize(): DeltaOptimizeBuilder = {
    io.delta.tables.connect.DeltaOptimizeBuilder(sparkSession, table)
  }

  private def executeUpdate(condition: Option[Column], set: Map[String, Column]): Unit = {
    val assignments = set.toSeq.map { case (field, value) =>
      proto.Assignment
        .newBuilder()
        .setField(toExpr(functions.expr(field)))
        .setValue(toExpr(value))
        .build()
    }
    val update = proto.UpdateTable
      .newBuilder()
      .setTarget(df.plan.getRoot)
      .addAllAssignments(assignments.asJava)
    condition.foreach(c => update.setCondition(toExpr(c)))
    val relation = proto.DeltaRelation.newBuilder().setUpdateTable(update).build()
    val extension = com.google.protobuf.Any.pack(relation)
    val sparkRelation = spark_proto.Relation.newBuilder().setExtension(extension).build()
    sparkSession.newDataFrame(_.mergeFrom(sparkRelation)).collect()
  }

  /** @inheritdoc */
  override def update(set: Map[String, Column]): Unit = {
    executeUpdate(condition = None, set)
  }

  /** @inheritdoc */
  override def update(set: java.util.Map[String, Column]): Unit = {
    update(set.asScala.toMap)
  }

  /** @inheritdoc */
  override def update(condition: Column, set: Map[String, Column]): Unit = {
    executeUpdate(Some(condition), set)
  }

  /** @inheritdoc */
  override def update(condition: Column, set: java.util.Map[String, Column]): Unit = {
    executeUpdate(Some(condition), set.asScala.toMap)
  }

  /** @inheritdoc */
  override def updateExpr(set: Map[String, String]): Unit = {
    update(toStrColumnMap(set))
  }

  /** @inheritdoc */
  override def updateExpr(set: java.util.Map[String, String]): Unit = {
    update(toStrColumnMap(set.asScala.toMap))
  }

  /** @inheritdoc */
  override def updateExpr(condition: String, set: Map[String, String]): Unit = {
    executeUpdate(Some(functions.expr(condition)), toStrColumnMap(set))
  }

  /** @inheritdoc */
  override def updateExpr(condition: String, set: java.util.Map[String, String]): Unit = {
    executeUpdate(Some(functions.expr(condition)), toStrColumnMap(set.asScala.toMap))
  }

  /** @inheritdoc */
  override def merge(source: DataFrame, condition: String): DeltaMergeBuilder = {
    merge(source, functions.expr(condition))
  }

  /** @inheritdoc */
  override def merge(source: DataFrame, condition: Column): DeltaMergeBuilder = {
    io.delta.tables.connect.DeltaMergeBuilder(this, source, condition)
  }

  private def executeClone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String],
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None): DeltaTable = {
    val clone = proto.CloneTable
      .newBuilder()
      .setTable(table)
      .setTarget(target)
      .setIsShallow(isShallow)
      .setReplace(replace)
      .putAllProperties(properties.asJava)
    versionAsOf.foreach(v => clone.setVersion(v.toInt))
    timestampAsOf.foreach(clone.setTimestamp)
    val command = proto.DeltaCommand.newBuilder().setCloneTable(clone).build()
    execute(command)
    DeltaTable.forPath(sparkSession, target)
  }

  /** @inheritdoc */
  override def clone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable = {
    executeClone(target, isShallow, replace, properties, versionAsOf = None, timestampAsOf = None)
  }

  /** @inheritdoc */
  override def clone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable = {
    val scalaProps = Option(properties).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    executeClone(target, isShallow, replace, scalaProps, versionAsOf = None, timestampAsOf = None)
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
      target, isShallow, replace, properties, versionAsOf = Some(version), timestampAsOf = None)
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
      target, isShallow, replace, scalaProps, versionAsOf = Some(version), timestampAsOf = None)
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
      target, isShallow, replace, properties, versionAsOf = None, timestampAsOf = Some(timestamp))
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
      target, isShallow, replace, scalaProps, versionAsOf = None, timestampAsOf = Some(timestamp))
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

  private def executeRestore(version: Option[Long], timestamp: Option[String]): DataFrame = {
    val restore = proto.RestoreTable
      .newBuilder()
      .setTable(table)
    version.foreach(restore.setVersion)
    timestamp.foreach(restore.setTimestamp)
    val relation = proto.DeltaRelation.newBuilder().setRestoreTable(restore).build()
    val extension = com.google.protobuf.Any.pack(relation)
    val sparkRelation = spark_proto.Relation.newBuilder().setExtension(extension).build()
    val result = sparkSession.newDataFrame(_.mergeFrom(sparkRelation)).collectResult()
    val data = try {
      result.toArray.toSeq.asJava
    } finally {
      result.close()
    }
    sparkSession.createDataFrame(data, result.schema)
  }

  /** @inheritdoc */
  override def restoreToVersion(version: Long): DataFrame = {
    executeRestore(version = Some(version), timestamp = None)
  }

  /** @inheritdoc */
  override def restoreToTimestamp(timestamp: String): DataFrame = {
    executeRestore(version = None, timestamp = Some(timestamp))
  }

  /** @inheritdoc */
  override def upgradeTableProtocol(readerVersion: Int, writerVersion: Int): Unit = {
    val upgrade = proto.UpgradeTableProtocol
      .newBuilder()
      .setTable(table)
      .setReaderVersion(readerVersion)
      .setWriterVersion(writerVersion)
    val command = proto.DeltaCommand.newBuilder().setUpgradeTableProtocol(upgrade).build()
    execute(command)
  }

  /** @inheritdoc */
  override def addFeatureSupport(featureName: String): Unit = {
    val addFeatureSupport = proto.AddFeatureSupport
      .newBuilder()
      .setTable(table)
      .setFeatureName(featureName)
    val command = proto.DeltaCommand.newBuilder().setAddFeatureSupport(addFeatureSupport).build()
    execute(command)
  }

  private def executeDropFeature(featureName: String, truncateHistory: Option[Boolean]): Unit = {
    val dropFeatureSupport = proto.DropFeatureSupport
      .newBuilder()
      .setTable(table)
      .setFeatureName(featureName)
    truncateHistory.foreach(dropFeatureSupport.setTruncateHistory)
    val command = proto.DeltaCommand.newBuilder().setDropFeatureSupport(dropFeatureSupport).build()
    execute(command)
  }

  /** @inheritdoc */
  override def dropFeatureSupport(featureName: String, truncateHistory: Boolean): Unit = {
    executeDropFeature(featureName, Some(truncateHistory))
  }

  /** @inheritdoc */
  override def dropFeatureSupport(featureName: String): Unit = {
    executeDropFeature(featureName, None)
  }

  private def execute(command: proto.DeltaCommand): Unit = {
    val extension = com.google.protobuf.Any.pack(command)
    val sparkCommand = spark_proto.Command
      .newBuilder()
      .setExtension(extension)
      .build()
    sparkSession.execute(sparkCommand)
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }
}

/**
 * Connect implementation companion object with factory methods.
 */
object DeltaTable extends DeltaTableCompanion {

  private def getActiveSparkSession(): SparkSession = {
    SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
  }

  /** @inheritdoc */
  override def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: StructType): DeltaTable = {
    throw new UnsupportedOperationException(
      "convertToDelta is not supported in Spark Connect mode")
  }

  /** @inheritdoc */
  override def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: String): DeltaTable = {
    throw new UnsupportedOperationException(
      "convertToDelta is not supported in Spark Connect mode")
  }

  /** @inheritdoc */
  override def convertToDelta(spark: SparkSession, identifier: String): DeltaTable = {
    throw new UnsupportedOperationException(
      "convertToDelta is not supported in Spark Connect mode")
  }

  /** @inheritdoc */
  override def forPath(path: String): DeltaTable = {
    forPath(getActiveSparkSession(), path)
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
    val table = proto.DeltaTable
      .newBuilder()
      .setPath(
        proto.DeltaTable.Path
          .newBuilder().setPath(path)
          .putAllHadoopConf(hadoopConf.asJava))
      .build()
    forTable(sparkSession, table)
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
    forName(getActiveSparkSession(), tableOrViewName)
  }

  /** @inheritdoc */
  override def forName(sparkSession: SparkSession, tableName: String): DeltaTable = {
    val table = proto.DeltaTable
      .newBuilder()
      .setTableOrViewName(tableName)
      .build()
    forTable(sparkSession, table)
  }

  private def forTable(sparkSession: SparkSession, table: proto.DeltaTable): DeltaTable = {
    val relation = proto.DeltaRelation
      .newBuilder()
      .setScan(proto.Scan.newBuilder().setTable(table))
      .build()
    val extension = com.google.protobuf.Any.pack(relation)
    val sparkRelation = spark_proto.Relation.newBuilder().setExtension(extension).build()
    val df = sparkSession.newDataFrame(_.mergeFrom(sparkRelation))
    new DeltaTable(df, table)
  }

  /** @inheritdoc */
  override def isDeltaTable(sparkSession: SparkSession, identifier: String): Boolean = {
    val relation = proto.DeltaRelation
      .newBuilder()
      .setIsDeltaTable(proto.IsDeltaTable.newBuilder().setPath(identifier))
      .build()
    val extension = com.google.protobuf.Any.pack(relation)
    val sparkRelation = spark_proto.Relation.newBuilder().setExtension(extension).build()
    sparkSession.newDataset(PrimitiveBooleanEncoder)(_.mergeFrom(sparkRelation)).head()
  }

  /** @inheritdoc */
  override def isDeltaTable(identifier: String): Boolean = {
    isDeltaTable(getActiveSparkSession(), identifier)
  }

  /** @inheritdoc */
  @Evolving
  override def create(): DeltaTableBuilder = {
    create(getActiveSparkSession())
  }

  /** @inheritdoc */
  @Evolving
  override def create(spark: SparkSession): DeltaTableBuilder = {
    new io.delta.tables.connect.DeltaTableBuilder(spark, CreateTableOptions(ifNotExists = false))
  }

  /** @inheritdoc */
  @Evolving
  override def createIfNotExists(): DeltaTableBuilder = {
    createIfNotExists(getActiveSparkSession())
  }

  /** @inheritdoc */
  @Evolving
  override def createIfNotExists(spark: SparkSession): DeltaTableBuilder = {
    new io.delta.tables.connect.DeltaTableBuilder(spark, CreateTableOptions(ifNotExists = true))
  }

  /** @inheritdoc */
  @Evolving
  override def replace(): DeltaTableBuilder = {
    replace(getActiveSparkSession())
  }

  /** @inheritdoc */
  @Evolving
  override def replace(spark: SparkSession): DeltaTableBuilder = {
    new io.delta.tables.connect.DeltaTableBuilder(spark, ReplaceTableOptions(orCreate = false))
  }

  /** @inheritdoc */
  @Evolving
  override def createOrReplace(): DeltaTableBuilder = {
    createOrReplace(getActiveSparkSession())
  }

  /** @inheritdoc */
  @Evolving
  override def createOrReplace(spark: SparkSession): DeltaTableBuilder = {
    new io.delta.tables.connect.DeltaTableBuilder(spark, ReplaceTableOptions(orCreate = true))
  }

  /** @inheritdoc */
  @Evolving
  override def columnBuilder(colName: String): DeltaColumnBuilder = {
    new io.delta.tables.connect.DeltaColumnBuilder(colName)
  }

  /** @inheritdoc */
  @Evolving
  override def columnBuilder(spark: SparkSession, colName: String): DeltaColumnBuilder = {
    new io.delta.tables.connect.DeltaColumnBuilder(colName)
  }

  private[tables] def createAnalysisException(message: String): AnalysisException = {
    new AnalysisException(
      errorClass = "ALL_PARTITION_COLUMNS_NOT_ALLOWED",
      messageParameters = Map("message" -> message)).copy(
        message = message,
        errorClass = None,
        messageParameters = Map.empty)
  }
}
