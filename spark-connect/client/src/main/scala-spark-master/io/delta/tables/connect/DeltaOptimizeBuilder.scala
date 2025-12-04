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

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._

/**
 * Connect (remote Spark) implementation of DeltaOptimizeBuilder.
 */
class DeltaOptimizeBuilder private(
    private val sparkSession: SparkSession,
    private val table: proto.DeltaTable)
  extends io.delta.tables.DeltaOptimizeBuilder {

  private var partitionFilters: Seq[String] = Seq.empty

  /** @inheritdoc */
  override def where(partitionFilter: String): DeltaOptimizeBuilder = {
    this.partitionFilters = this.partitionFilters :+ partitionFilter
    this
  }

  /** @inheritdoc */
  override def executeCompaction(): DataFrame = {
    execute(Seq.empty)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  override def executeZOrderBy(columns: String*): DataFrame = {
    execute(columns)
  }

  private def execute(zOrderBy: Seq[String]): DataFrame = {
    val optimize = proto.OptimizeTable
      .newBuilder()
      .setTable(table)
      .addAllPartitionFilters(partitionFilters.asJava)
      .addAllZorderColumns(zOrderBy.asJava)
    val relation = proto.DeltaRelation.newBuilder().setOptimizeTable(optimize).build()
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
}

private[delta] object DeltaOptimizeBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      sparkSession: SparkSession,
      table: proto.DeltaTable): DeltaOptimizeBuilder = {
    new DeltaOptimizeBuilder(sparkSession, table)
  }
}
