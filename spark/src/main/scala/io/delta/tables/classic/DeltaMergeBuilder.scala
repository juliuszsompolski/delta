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
import scala.collection.Map

import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.{
  DeltaAnalysisException,
  PostHocResolveUpCast,
  PreprocessTableMerge,
  ResolveDeltaMergeInto
}
import org.apache.spark.sql.delta.DeltaTableUtils.withActiveSession
import org.apache.spark.sql.delta.DeltaViewHelper
import org.apache.spark.sql.delta.util.AnalysisHelper
import io.delta.tables.{
  DeltaMergeBuilder => DeltaMergeBuilderAPI,
  DeltaMergeMatchedActionBuilder => DeltaMergeMatchedActionBuilderAPI,
  DeltaMergeNotMatchedActionBuilder => DeltaMergeNotMatchedActionBuilderAPI,
  DeltaMergeNotMatchedBySourceActionBuilder => DeltaMergeNotMatchedBySourceActionBuilderAPI
}

import org.apache.spark.annotation._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf

/**
 * Classic (local Spark) implementation of DeltaMergeBuilder.
 */
class DeltaMergeBuilder private(
    private val targetTable: DeltaTable,
    private val source: DataFrame,
    private val onCondition: Column,
    private val whenClauses: Seq[DeltaMergeIntoClause],
    private val schemaEvolutionEnabled: Boolean)
  extends DeltaMergeBuilderAPI
  with AnalysisHelper
  with Logging {

  def this(
      targetTable: DeltaTable,
      source: DataFrame,
      onCondition: Column,
      whenClauses: Seq[DeltaMergeIntoClause]) =
    this(targetTable, source, onCondition, whenClauses, schemaEvolutionEnabled = false)

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
      this.whenClauses,
      schemaEvolutionEnabled = true)
  }

  /** @inheritdoc */
  override def execute(): DataFrame = improveUnsupportedOpError {
    val sparkSession = targetTable.toDF.sparkSession
    withActiveSession(sparkSession) {
      val resolvedMergeInto =
        ResolveDeltaMergeInto.resolveReferencesAndSchema(
          mergePlan, sparkSession.sessionState.conf)(
          tryResolveReferencesForExpressions(sparkSession))

      val strippedMergeInto = resolvedMergeInto.copy(
        target = DeltaViewHelper.stripTempViewForMerge(resolvedMergeInto.target, SQLConf.get)
      )
      var mergeIntoCommand =
        PreprocessTableMerge(sparkSession.sessionState.conf)(strippedMergeInto)
      mergeIntoCommand = PostHocResolveUpCast(sparkSession).apply(mergeIntoCommand)
      sparkSession.sessionState.analyzer.checkAnalysis(mergeIntoCommand)
      toDataset(sparkSession, mergeIntoCommand)
    }
  }

  /**
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[tables] def withClause(clause: DeltaMergeIntoClause): DeltaMergeBuilder = {
    new DeltaMergeBuilder(
      this.targetTable,
      this.source,
      this.onCondition,
      this.whenClauses :+ clause,
      this.schemaEvolutionEnabled)
  }

  private def mergePlan: DeltaMergeInto = {
    var targetPlan = targetTable.toDF.queryExecution.analyzed
    var sourcePlan = source.queryExecution.analyzed
    var condition = onCondition.expr
    var clauses = whenClauses

    val duplicateResolvedRefs = targetPlan.outputSet.intersect(sourcePlan.outputSet)
    if (duplicateResolvedRefs.nonEmpty) {
      val exprs = (condition +: clauses).map(_.transform {
        case a: AttributeReference if duplicateResolvedRefs.contains(a) =>
          UnresolvedAttribute(a.qualifier :+ a.name)
      })
      val fakePlan = AnalysisHelper.FakeLogicalPlan(exprs, Seq(sourcePlan, targetPlan))
      val newPlan = org.apache.spark.sql.catalyst.analysis.DeduplicateRelations(fakePlan)
        .asInstanceOf[AnalysisHelper.FakeLogicalPlan]
      sourcePlan = newPlan.children(0)
      targetPlan = newPlan.children(1)
      condition = newPlan.exprs.head
      clauses = newPlan.exprs.takeRight(clauses.size).asInstanceOf[Seq[DeltaMergeIntoClause]]
    }

    val merge = DeltaMergeInto(
      targetPlan, sourcePlan, condition, clauses, withSchemaEvolution = schemaEvolutionEnabled)
    logDebug("Generated merged plan:\n" + merge)
    merge
  }
}

object DeltaMergeBuilder {
  /**
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[tables] def apply(
      targetTable: DeltaTable,
      source: DataFrame,
      onCondition: Column): DeltaMergeBuilder = {
    new DeltaMergeBuilder(targetTable, source, onCondition, Nil)
  }
}

/**
 * Classic (local Spark) implementation of DeltaMergeMatchedActionBuilder.
 */
class DeltaMergeMatchedActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val matchCondition: Option[Column])
  extends DeltaMergeMatchedActionBuilderAPI {

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
  override def updateAll(): DeltaMergeBuilder = {
    val updateClause = DeltaMergeIntoMatchedUpdateClause(
      matchCondition.map(_.expr),
      DeltaMergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.withClause(updateClause)
  }

  /** @inheritdoc */
  override def delete(): DeltaMergeBuilder = {
    val deleteClause = DeltaMergeIntoMatchedDeleteClause(matchCondition.map(_.expr))
    mergeBuilder.withClause(deleteClause)
  }

  private def addUpdateClause(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty && matchCondition.isEmpty) {
      mergeBuilder
    } else {
      val setActions = set.toSeq
      val updateActions = DeltaMergeIntoClause.toActions(
        colNames = setActions.map(x => UnresolvedAttribute.quotedString(x._1)),
        exprs = setActions.map(x => x._2.expr),
        isEmptySeqEqualToStar = false)
      val updateClause = DeltaMergeIntoMatchedUpdateClause(
        matchCondition.map(_.expr),
        updateActions)
      mergeBuilder.withClause(updateClause)
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr(_)).toMap
}

object DeltaMergeMatchedActionBuilder {
  /**
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[tables] def apply(
      mergeBuilder: DeltaMergeBuilder,
      matchCondition: Option[Column]): DeltaMergeMatchedActionBuilder = {
    new DeltaMergeMatchedActionBuilder(mergeBuilder, matchCondition)
  }
}

/**
 * Classic (local Spark) implementation of DeltaMergeNotMatchedActionBuilder.
 */
class DeltaMergeNotMatchedActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val notMatchCondition: Option[Column])
  extends DeltaMergeNotMatchedActionBuilderAPI {

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
    val insertClause = DeltaMergeIntoNotMatchedInsertClause(
      notMatchCondition.map(_.expr),
      DeltaMergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.withClause(insertClause)
  }

  private def addInsertClause(setValues: Map[String, Column]): DeltaMergeBuilder = {
    val values = setValues.toSeq
    val insertActions = DeltaMergeIntoClause.toActions(
      colNames = values.map(x => UnresolvedAttribute.quotedString(x._1)),
      exprs = values.map(x => x._2.expr),
      isEmptySeqEqualToStar = false)
    val insertClause = DeltaMergeIntoNotMatchedInsertClause(
      notMatchCondition.map(_.expr),
      insertActions)
    mergeBuilder.withClause(insertClause)
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr(_)).toMap
}

object DeltaMergeNotMatchedActionBuilder {
  /**
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[tables] def apply(
      mergeBuilder: DeltaMergeBuilder,
      notMatchCondition: Option[Column]): DeltaMergeNotMatchedActionBuilder = {
    new DeltaMergeNotMatchedActionBuilder(mergeBuilder, notMatchCondition)
  }
}

/**
 * Classic (local Spark) implementation of DeltaMergeNotMatchedBySourceActionBuilder.
 */
class DeltaMergeNotMatchedBySourceActionBuilder private(
    private val mergeBuilder: DeltaMergeBuilder,
    private val notMatchBySourceCondition: Option[Column])
  extends DeltaMergeNotMatchedBySourceActionBuilderAPI {

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
    val deleteClause =
      DeltaMergeIntoNotMatchedBySourceDeleteClause(notMatchBySourceCondition.map(_.expr))
    mergeBuilder.withClause(deleteClause)
  }

  private def addUpdateClause(set: Map[String, Column]): DeltaMergeBuilder = {
    if (set.isEmpty && notMatchBySourceCondition.isEmpty) {
      mergeBuilder
    } else {
      val setActions = set.toSeq
      val updateActions = DeltaMergeIntoClause.toActions(
        colNames = setActions.map(x => UnresolvedAttribute.quotedString(x._1)),
        exprs = setActions.map(x => x._2.expr),
        isEmptySeqEqualToStar = false)
      val updateClause = DeltaMergeIntoNotMatchedBySourceUpdateClause(
        notMatchBySourceCondition.map(_.expr),
        updateActions)
      mergeBuilder.withClause(updateClause)
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] =
    map.mapValues(functions.expr(_)).toMap
}

object DeltaMergeNotMatchedBySourceActionBuilder {
  /**
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[tables] def apply(
      mergeBuilder: DeltaMergeBuilder,
      notMatchBySourceCondition: Option[Column]): DeltaMergeNotMatchedBySourceActionBuilder = {
    new DeltaMergeNotMatchedBySourceActionBuilder(mergeBuilder, notMatchBySourceCondition)
  }
}
