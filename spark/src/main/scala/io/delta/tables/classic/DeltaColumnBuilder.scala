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

import org.apache.spark.sql.delta.{DeltaErrors, IdentityColumn}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.{
  GENERATION_EXPRESSION_METADATA_KEY,
  IDENTITY_INFO_ALLOW_EXPLICIT_INSERT,
  IDENTITY_INFO_START,
  IDENTITY_INFO_STEP
}
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.annotation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, LongType, MetadataBuilder, StructField}

/**
 * Classic (local Spark) implementation of DeltaColumnBuilder.
 */
@Evolving
class DeltaColumnBuilder private[tables](
    private val spark: SparkSession,
    private val colName: String)
  extends io.delta.tables.DeltaColumnBuilder {

  private var columnDataType: DataType = _
  private var columnNullable: Boolean = true
  private var generationExpr: Option[String] = None
  private var columnComment: Option[String] = None
  private var identityStart: Option[Long] = None
  private var identityStep: Option[Long] = None
  private var identityAllowExplicitInsert: Option[Boolean] = None

  /** @inheritdoc */
  @Evolving
  override def dataType(dataType: String): DeltaColumnBuilder = {
    this.columnDataType = spark.sessionState.sqlParser.parseDataType(dataType)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def dataType(dataType: DataType): DeltaColumnBuilder = {
    this.columnDataType = dataType
    this
  }

  /** @inheritdoc */
  @Evolving
  override def nullable(nullable: Boolean): DeltaColumnBuilder = {
    this.columnNullable = nullable
    this
  }

  /** @inheritdoc */
  @Evolving
  override def generatedAlwaysAs(expr: String): DeltaColumnBuilder = {
    this.generationExpr = Option(expr)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def generatedAlwaysAsIdentity(): DeltaColumnBuilder = {
    generatedAlwaysAsIdentity(IdentityColumn.defaultStart, IdentityColumn.defaultStep)
  }

  /** @inheritdoc */
  @Evolving
  override def generatedAlwaysAsIdentity(start: Long, step: Long): DeltaColumnBuilder = {
    this.identityStart = Some(start)
    this.identityStep = Some(step)
    this.identityAllowExplicitInsert = Some(false)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def generatedByDefaultAsIdentity(): DeltaColumnBuilder = {
    generatedByDefaultAsIdentity(IdentityColumn.defaultStart, IdentityColumn.defaultStep)
  }

  /** @inheritdoc */
  @Evolving
  override def generatedByDefaultAsIdentity(start: Long, step: Long): DeltaColumnBuilder = {
    this.identityStart = Some(start)
    this.identityStep = Some(step)
    this.identityAllowExplicitInsert = Some(true)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def comment(comment: String): DeltaColumnBuilder = {
    this.columnComment = Option(comment)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def build(): StructField = {
    val metadataBuilder = new MetadataBuilder()
    if (generationExpr.nonEmpty) {
      metadataBuilder.putString(GENERATION_EXPRESSION_METADATA_KEY, generationExpr.get)
    }

    identityAllowExplicitInsert.ifDefined { allowExplicitInsert =>
      if (generationExpr.nonEmpty) {
        throw DeltaErrors.identityColumnWithGenerationExpression()
      }

      if (columnDataType != null && columnDataType != LongType) {
        throw DeltaErrors.identityColumnDataTypeNotSupported(columnDataType)
      }

      metadataBuilder.putBoolean(
        IDENTITY_INFO_ALLOW_EXPLICIT_INSERT, allowExplicitInsert)
      metadataBuilder.putLong(IDENTITY_INFO_START, identityStart.get)
      val step = identityStep.get
      if (step == 0L) {
        throw DeltaErrors.identityColumnIllegalStep()
      }
      metadataBuilder.putLong(IDENTITY_INFO_STEP, identityStep.get)
    }

    if (columnComment.nonEmpty) {
      metadataBuilder.putString("comment", columnComment.get)
    }
    val fieldMetadata = metadataBuilder.build()
    if (columnDataType == null) {
      throw DeltaErrors.columnBuilderMissingDataType(colName)
    }
    StructField(
      colName,
      columnDataType,
      nullable = columnNullable,
      metadata = fieldMetadata)
  }
}
