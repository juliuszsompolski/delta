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
import scala.collection.mutable

import io.delta.connect.proto
import io.delta.connect.spark.{proto => spark_proto}
import io.delta.tables.{
  DeltaTable,
  DeltaTableBuilderOptions,
  CreateTableOptions,
  ReplaceTableOptions
}

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Connect (remote Spark) implementation of DeltaTableBuilder.
 */
@Evolving
class DeltaTableBuilder private[tables](
    spark: SparkSession,
    builderOption: DeltaTableBuilderOptions)
  extends io.delta.tables.DeltaTableBuilder {

  private var identifier: Option[String] = None
  private var partitioningColumns: Seq[String] = Nil
  private var clusteringColumns: Seq[String] = Nil
  private var columns: mutable.Seq[StructField] = mutable.Seq.empty
  private var tableLocation: Option[String] = None
  private var tblComment: Option[String] = None
  private var properties = Map.empty[String, String]

  /** @inheritdoc */
  @Evolving
  override def tableName(identifier: String): DeltaTableBuilder = {
    this.identifier = Some(identifier)
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
      io.delta.tables.connect.DeltaTable.columnBuilder(spark, colName).dataType(dataType).build()
    )
    this
  }

  /** @inheritdoc */
  @Evolving
  override def addColumn(colName: String, dataType: DataType): DeltaTableBuilder = {
    addColumn(
      io.delta.tables.connect.DeltaTable.columnBuilder(spark, colName).dataType(dataType).build()
    )
    this
  }

  /** @inheritdoc */
  @Evolving
  override def addColumn(colName: String, dataType: String, nullable: Boolean): DeltaTableBuilder = {
    addColumn(
      io.delta.tables.connect.DeltaTable
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
      io.delta.tables.connect.DeltaTable
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
    columns = columns ++ cols
    this
  }

  /** @inheritdoc */
  @Evolving
  @scala.annotation.varargs
  override def partitionedBy(colNames: String*): DeltaTableBuilder = {
    partitioningColumns = colNames
    this
  }

  /** @inheritdoc */
  @Evolving
  @scala.annotation.varargs
  override def clusterBy(colNames: String*): DeltaTableBuilder = {
    clusteringColumns = colNames
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
  override def execute(): DeltaTable = {
    if (identifier.isEmpty && tableLocation.isEmpty) {
      val exMessage = "Table name or location has to be specified"
      throw io.delta.tables.connect.DeltaTable.createAnalysisException(exMessage)
    }

    val mode = builderOption match {
      case CreateTableOptions(ifNotExists) =>
        if (ifNotExists) proto.CreateDeltaTable.Mode.MODE_CREATE_IF_NOT_EXISTS
        else proto.CreateDeltaTable.Mode.MODE_CREATE
      case ReplaceTableOptions(orCreate) =>
        if (orCreate) proto.CreateDeltaTable.Mode.MODE_CREATE_OR_REPLACE
        else proto.CreateDeltaTable.Mode.MODE_REPLACE
    }

    val createDeltaTable = proto.CreateDeltaTable
      .newBuilder()
      .setMode(mode)
      .addAllPartitioningColumns(partitioningColumns.asJava)
      .addAllClusteringColumns(clusteringColumns.asJava)
      .putAllProperties(properties.asJava)
    identifier.foreach(createDeltaTable.setTableName)
    tableLocation.foreach(createDeltaTable.setLocation)
    tblComment.foreach(createDeltaTable.setComment)

    val protoColumns = columns.map { f =>
      val builder = proto.CreateDeltaTable.Column
        .newBuilder()
        .setName(f.name)
        .setDataType(DataTypeProtoConverter.toConnectProtoType(f.dataType))
        .setNullable(f.nullable)
      if (f.metadata.contains("delta.generationExpression")) {
        builder.setGeneratedAlwaysAs(f.metadata.getString("delta.generationExpression"))
      }
      if (f.metadata.contains("delta.identity.allowExplicitInsert")) {
        builder.setIdentityInfo(
          proto.CreateDeltaTable.Column.IdentityInfo
            .newBuilder()
            .setStart(f.metadata.getLong("delta.identity.start"))
            .setStep(f.metadata.getLong("delta.identity.step"))
            .setAllowExplicitInsert(f.metadata.getBoolean("delta.identity.allowExplicitInsert"))
            .build()
        )
      }
      if (f.metadata.contains("comment")) {
        builder.setComment(f.metadata.getString("comment"))
      }
      builder.build()
    }
    createDeltaTable.addAllColumns(protoColumns.asJava)

    val command = proto.DeltaCommand.newBuilder().setCreateDeltaTable(createDeltaTable).build()
    val extension = com.google.protobuf.Any.pack(command)
    val sparkCommand = spark_proto.Command.newBuilder().setExtension(extension).build()
    spark.execute(sparkCommand)

    if (tableLocation.isDefined) {
      io.delta.tables.connect.DeltaTable.forPath(spark, tableLocation.get)
    } else {
      io.delta.tables.connect.DeltaTable.forName(spark, identifier.get)
    }
  }
}
