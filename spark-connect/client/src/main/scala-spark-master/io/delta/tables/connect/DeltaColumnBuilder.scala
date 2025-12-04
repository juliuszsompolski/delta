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

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.catalyst.parser.DataTypeParser
import org.apache.spark.sql.types.{DataType, LongType, MetadataBuilder, StructField}

/**
 * Connect (remote Spark) implementation of DeltaColumnBuilder.
 */
@Evolving
class DeltaColumnBuilder private[tables](private val colName: String)
  extends io.delta.tables.DeltaColumnBuilder {

  private var dataTypeValue: DataType = _
  private var nullableValue: Boolean = true
  private var generationExpr: Option[String] = None
  private var commentValue: Option[String] = None
  private var identityStart: Option[Long] = None
  private var identityStep: Option[Long] = None
  private var identityAllowExplicitInsert: Option[Boolean] = None

  /** @inheritdoc */
  @Evolving
  override def dataType(dataType: String): DeltaColumnBuilder = {
    this.dataTypeValue = DataTypeParser.parseDataType(dataType)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def dataType(dataType: DataType): DeltaColumnBuilder = {
    this.dataTypeValue = dataType
    this
  }

  /** @inheritdoc */
  @Evolving
  override def nullable(nullable: Boolean): DeltaColumnBuilder = {
    this.nullableValue = nullable
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
    generatedAlwaysAsIdentity(start = 1, step = 1)
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
    generatedByDefaultAsIdentity(start = 1, step = 1)
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
    this.commentValue = Option(comment)
    this
  }

  /** @inheritdoc */
  @Evolving
  override def build(): StructField = {
    val metadataBuilder = new MetadataBuilder()
    if (generationExpr.nonEmpty) {
      metadataBuilder.putString("delta.generationExpression", generationExpr.get)
    }

    identityAllowExplicitInsert.foreach { allowExplicitInsert =>
      if (generationExpr.nonEmpty) {
        throw io.delta.tables.connect.DeltaTable.createAnalysisException(
          "IDENTITY column cannot be specified with a generated column expression.")
      }

      if (dataTypeValue != null && dataTypeValue != LongType) {
        throw io.delta.tables.connect.DeltaTable.createAnalysisException(
          s"DataType ${dataTypeValue.typeName} is not supported for IDENTITY columns.")
      }

      metadataBuilder.putBoolean("delta.identity.allowExplicitInsert", allowExplicitInsert)
      metadataBuilder.putLong("delta.identity.start", identityStart.get)
      if (identityStep.get == 0L) {
        throw io.delta.tables.connect.DeltaTable.createAnalysisException(
          "IDENTITY column step cannot be 0.")
      }
      metadataBuilder.putLong("delta.identity.step", identityStep.get)
    }

    if (commentValue.nonEmpty) {
      metadataBuilder.putString("comment", commentValue.get)
    }
    val fieldMetadata = metadataBuilder.build()
    if (dataTypeValue == null) {
      throw io.delta.tables.connect.DeltaTable.createAnalysisException(
        s"The data type of the column $colName is not provided")
    }
    StructField(
      colName,
      dataTypeValue,
      nullable = nullableValue,
      metadata = fieldMetadata)
  }
}
