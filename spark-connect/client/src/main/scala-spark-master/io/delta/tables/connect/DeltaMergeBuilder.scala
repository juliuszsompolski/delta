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

import java.util.Arrays

import scala.collection.JavaConverters._
import scala.collection.Map

import io.delta.connect.proto
import io.delta.connect.spark.{proto => spark_proto}

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.{functions, Column, DataFrame}
import org.apache.spark.sql.connect.ColumnNodeToProtoConverter.toExpr
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._
import org.apache.spark.sql.functions.expr

/**
 * Connect (remote Spark) implementation of DeltaMergeBuilder.
 */
class DeltaMergeBuilder private(
    private val targetTable: DeltaTable,
    private val source: DataFrame,
    private val onCondition: Column,
    private val whenMatchedClauses: Seq[proto.MergeIntoTable.Action],
    private val whenNotMatchedClauses: Seq[proto.MergeIntoTable.Action],
    private val whenNotMatchedBySourceClauses: Seq[proto.MergeIntoTable.Action],
    private val schemaEvolutionEnabled: Boolean)
  extends io.delta.tables.DeltaMergeBuilder {

  // Schema Evolution is off by default in Merge.
  def this(
      targetTable: DeltaTable,
      source: DataFrame,
      onCondition: Column,
      whenMatchedClauses: Seq[proto.MergeIntoTable.Action],
      whenNotMatchedClauses: Seq[proto.MergeIntoTable.Action],
      whenNotMatchedBySourceClauses: Seq[proto.MergeIntoTable.Action]) =
    this(targetTable, source, onCondition, whenMatchedClauses,
      whenNotMatchedClauses, whenNotMatchedBySourceClauses, schemaEvolutionEnabled = false)

  /** @inheritdoc */
  override def whenMatched(): DeltaMergeMatchedActionBuilder = {
    DeltaMergeMatchedActionBuilder(this, None)
  }

  /** @inheritdoc */
  override def whenMatched(condition: String): DeltaMergeMatchedActionBuilder = {
    whenMatched(expr(condition))
  }

  /** @inheritdoc */
  override def whenMatched(condition: Column): DeltaMergeMatchedActionBuilder = {
    DeltaMergeMatchedActionBuilder(this, Some(condition))
  }

  /** @inheritdoc */
  override def whenNotMatched(): DeltaMergeNotMatchedActionBuilder = {
    DeltaMergeNotMatchedActionBuilder(this, None)
  }

  /** @inheritdoc */
  override def whenNotMatched(condition: String): DeltaMergeNotMatchedActionBuilder = {
    whenNotMatched(expr(condition))
  }

  /** @inheritdoc */
  override def whenNotMatched(condition: Column): DeltaMergeNotMatchedActionBuilder = {
    DeltaMergeNotMatchedActionBuilder(this, Some(condition))
  }

  /** @inheritdoc */
  override def whenNotMatchedBySource(): DeltaMergeNotMatchedBySourceActionBuilder = {
    DeltaMergeNotMatchedBySourceActionBuilder(this, None)
  }

  /** @inheritdoc */
  override def whenNotMatchedBySource(
      condition: String): DeltaMergeNotMatchedBySourceActionBuilder = {
    whenNotMatchedBySource(expr(condition))
  }

  /** @inheritdoc */
  override def whenNotMatchedBySource(
      condition: Column): DeltaMergeNotMatchedBySourceActionBuilder = {
    DeltaMergeNotMatchedBySourceActionBuilder(this, Some(condition))
  }

  /** @inheritdoc */
  override def withSchemaEvolution(): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenMatchedClauses,
      this.whenNotMatchedClauses,
      this.whenNotMatchedBySourceClauses,
      schemaEvolutionEnabled = true)
  }

  /** @inheritdoc */
  override def execute(): DataFrame = {
    val sparkSession = targetTable.toDF.sparkSession
    val merge = proto.MergeIntoTable
      .newBuilder()
      .setTarget(targetTable.toDF.plan.getRoot)
      .setSource(source.plan.getRoot)
      .setCondition(toExpr(onCondition))
      .addAllMatchedActions(whenMatchedClauses.asJava)
      .addAllNotMatchedActions(whenNotMatchedClauses.asJava)
      .addAllNotMatchedBySourceActions(whenNotMatchedBySourceClauses.asJava)
      .setWithSchemaEvolution(schemaEvolutionEnabled)
    val relation = proto.DeltaRelation.newBuilder().setMergeIntoTable(merge).build()
    val extension = com.google.protobuf.Any.pack(relation)
    val sparkRelation = spark_proto.Relation.newBuilder().setExtension(extension).build()
    val resultDf = sparkSession.newDataFrame(_.mergeFrom(sparkRelation))
    val resultSchema = resultDf.schema
    // Ensure this is actually executed instead of just passing the DataFrame directly back to
    // the caller, in case they just drop it. The return type used to be Unit so dropping is
    // likely common.
    val result = resultDf.collect()
    sparkSession.createDataFrame(Arrays.asList(result: _*), resultSchema)
  }

  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def withWhenMatchedClause(
      clause: proto.MergeIntoTable.Action): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenMatchedClauses :+ clause,
      this.whenNotMatchedClauses,
      this.whenNotMatchedBySourceClauses,
      this.schemaEvolutionEnabled)
  }

  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def withWhenNotMatchedClause(
      clause: proto.MergeIntoTable.Action): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenMatchedClauses,
      this.whenNotMatchedClauses :+ clause,
      this.whenNotMatchedBySourceClauses,
      this.schemaEvolutionEnabled)
  }

  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def withWhenNotMatchedBySourceClause(
      clause: proto.MergeIntoTable.Action): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenMatchedClauses,
      this.whenNotMatchedClauses,
      this.whenNotMatchedBySourceClauses :+ clause,
      this.schemaEvolutionEnabled)
  }
}

object DeltaMergeBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      targetTable: DeltaTable,
      source: DataFrame,
      onCondition: Column): DeltaMergeBuilder = {
    new DeltaMergeBuilder(targetTable, source, onCondition, Nil, Nil, Nil)
  }
}

/**
 * Connect (remote Spark) implementation of DeltaMergeMatchedActionBuilder.
 */
class DeltaMergeMatchedActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val matchCondition: Option[Column])
  extends io.delta.tables.DeltaMergeMatchedActionBuilder {

  /** @inheritdoc */
  override def update(set: Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set)
  }

  /** @inheritdoc */
  override def updateExpr(set: Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set))
  }

  /** @inheritdoc */
  override def update(set: java.util.Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set.asScala.toMap)
  }

  /** @inheritdoc */
  override def updateExpr(set: java.util.Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set.asScala.toMap))
  }

  /** @inheritdoc */
  override def updateAll(): DeltaMergeBuilder = {
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setUpdateStarAction(proto.MergeIntoTable.Action.UpdateStarAction.newBuilder())
    matchCondition.foreach(c => clause.setCondition(toExpr(c)))
    mergeBuilder.withWhenMatchedClause(clause.build())
  }

  /** @inheritdoc */
  override def delete(): DeltaMergeBuilder = {
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setDeleteAction(proto.MergeIntoTable.Action.DeleteAction.newBuilder())
    matchCondition.foreach(c => clause.setCondition(toExpr(c)))
    mergeBuilder.withWhenMatchedClause(clause.build())
  }

  private def addUpdateClause(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty && matchCondition.isEmpty) {
      // This is a catch all clause that doesn't update anything: we can ignore it.
      mergeBuilder
    } else {
      val assignments = set.map { case (field, value) =>
        proto.Assignment.newBuilder().setField(toExpr(expr(field))).setValue(toExpr(value)).build()
      }
      val action = proto.MergeIntoTable.Action.UpdateAction
        .newBuilder()
        .addAllAssignments(assignments.asJava)
      val clause = proto.MergeIntoTable.Action
        .newBuilder()
        .setUpdateAction(action)
      matchCondition.foreach(c => clause.setCondition(toExpr(c)))
      mergeBuilder.withWhenMatchedClause(clause.build())
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr).toMap
}

object DeltaMergeMatchedActionBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      mergeBuilder: DeltaMergeBuilder,
      matchCondition: Option[Column]): DeltaMergeMatchedActionBuilder = {
    new DeltaMergeMatchedActionBuilder(mergeBuilder, matchCondition)
  }
}


/**
 * Connect (remote Spark) implementation of DeltaMergeNotMatchedActionBuilder.
 */
class DeltaMergeNotMatchedActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val notMatchCondition: Option[Column])
  extends io.delta.tables.DeltaMergeNotMatchedActionBuilder {

  /** @inheritdoc */
  override def insert(values: Map[String, Column]): DeltaMergeBuilder = {
    addInsertClause(values)
  }

  /** @inheritdoc */
  override def insertExpr(values: Map[String, String]): DeltaMergeBuilder = {
    addInsertClause(toStrColumnMap(values))
  }

  /** @inheritdoc */
  override def insert(values: java.util.Map[String, Column]): DeltaMergeBuilder = {
    addInsertClause(values.asScala)
  }

  /** @inheritdoc */
  override def insertExpr(values: java.util.Map[String, String]): DeltaMergeBuilder = {
    addInsertClause(toStrColumnMap(values.asScala))
  }

  /** @inheritdoc */
  override def insertAll(): DeltaMergeBuilder = {
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setInsertStarAction(proto.MergeIntoTable.Action.InsertStarAction.newBuilder())
    notMatchCondition.foreach(c => clause.setCondition(toExpr(c)))
    mergeBuilder.withWhenNotMatchedClause(clause.build())
  }

  private def addInsertClause(setValues: Map[String, Column]): DeltaMergeBuilder = {
    val assignments = setValues.map { case (field, value) =>
      proto.Assignment.newBuilder().setField(toExpr(expr(field))).setValue(toExpr(value)).build()
    }
    val action = proto.MergeIntoTable.Action.InsertAction
      .newBuilder()
      .addAllAssignments(assignments.asJava)
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setInsertAction(action)
    notMatchCondition.foreach(c => clause.setCondition(toExpr(c)))
    mergeBuilder.withWhenNotMatchedClause(clause.build())
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr).toMap
}

object DeltaMergeNotMatchedActionBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      mergeBuilder: DeltaMergeBuilder,
      notMatchCondition: Option[Column]): DeltaMergeNotMatchedActionBuilder = {
    new DeltaMergeNotMatchedActionBuilder(mergeBuilder, notMatchCondition)
  }
}

/**
 * Connect (remote Spark) implementation of DeltaMergeNotMatchedBySourceActionBuilder.
 */
class DeltaMergeNotMatchedBySourceActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val notMatchBySourceCondition: Option[Column])
  extends io.delta.tables.DeltaMergeNotMatchedBySourceActionBuilder {

  /** @inheritdoc */
  override def update(set: Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set)
  }

  /** @inheritdoc */
  override def updateExpr(set: Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set))
  }

  /** @inheritdoc */
  override def update(set: java.util.Map[String, Column]): DeltaMergeBuilder = {
    addUpdateClause(set.asScala)
  }

  /** @inheritdoc */
  override def updateExpr(set: java.util.Map[String, String]): DeltaMergeBuilder = {
    addUpdateClause(toStrColumnMap(set.asScala))
  }

  /** @inheritdoc */
  override def delete(): DeltaMergeBuilder = {
    val clause = proto.MergeIntoTable.Action
      .newBuilder()
      .setDeleteAction(proto.MergeIntoTable.Action.DeleteAction.newBuilder())
    notMatchBySourceCondition.foreach(c => clause.setCondition(toExpr(c)))
    mergeBuilder.withWhenNotMatchedBySourceClause(clause.build())
  }

  private def addUpdateClause(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty && notMatchBySourceCondition.isEmpty) {
      // This is a catch all clause that doesn't update anything: we can ignore it.
      mergeBuilder
    } else {
      val assignments = set.map { case (field, value) =>
        proto.Assignment.newBuilder().setField(toExpr(expr(field))).setValue(toExpr(value)).build()
      }
      val action = proto.MergeIntoTable.Action.UpdateAction
        .newBuilder()
        .addAllAssignments(assignments.asJava)
      val clause = proto.MergeIntoTable.Action
        .newBuilder()
        .setUpdateAction(action)
      notMatchBySourceCondition.foreach(c => clause.setCondition(toExpr(c)))
      mergeBuilder.withWhenNotMatchedBySourceClause(clause.build())
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr).toMap
}

object DeltaMergeNotMatchedBySourceActionBuilder {
  /**
   * :: Unstable ::
   *
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[delta] def apply(
      mergeBuilder: DeltaMergeBuilder,
      notMatchBySourceCondition: Option[Column]): DeltaMergeNotMatchedBySourceActionBuilder = {
    new DeltaMergeNotMatchedBySourceActionBuilder(mergeBuilder, notMatchBySourceCondition)
  }
}
