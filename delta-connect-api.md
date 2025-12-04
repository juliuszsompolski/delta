# Delta Connect API Refactoring Plan

## Overview

This document outlines the plan to refactor Delta's API structure to follow Spark's three-layer pattern:

1. **Public API Layer** (`io.delta.tables`) - Abstract definitions
2. **Classic Implementation** (`io.delta.tables.classic`) - Local Spark execution
3. **Connect Implementation** (`io.delta.tables.connect`) - Remote Spark Connect execution

## Current State

### Classic Implementation
- Location: `spark/src/main/scala/io/delta/tables/`
- Package: `io.delta.tables`
- Key classes: `DeltaTable`, `DeltaTableBuilder`, `DeltaColumnBuilder`, `DeltaMergeBuilder`, `DeltaOptimizeBuilder`
- Implementation: Uses `DeltaTableV2` and local Spark SQL catalyst execution

### Connect Implementation
- Location: `spark-connect/client/src/main/scala-spark-master/io/delta/connect/tables/`
- Package: `io.delta.connect.tables`
- Key classes: Same names as classic
- Implementation: Uses protobuf messages and Spark Connect RPC

### Problems with Current Structure
1. Both implementations use `io.delta.tables` as the user-facing package (Connect uses `io.delta.connect.tables`)
2. Cannot have both implementations available simultaneously
3. No shared API contract between implementations
4. Code duplication for common builder options and patterns

---

## Target Architecture

```
delta/
├── spark-api/                              # NEW: Public API module
│   └── src/main/scala/io/delta/tables/
│       ├── DeltaTable.scala               # Abstract class
│       ├── DeltaTableBuilder.scala        # Abstract class
│       ├── DeltaColumnBuilder.scala       # Abstract class
│       ├── DeltaMergeBuilder.scala        # Abstract classes
│       ├── DeltaOptimizeBuilder.scala     # Abstract class
│       └── package.scala                  # Type aliases, implicits
│
├── spark/                                  # Classic implementation
│   └── src/main/scala/io/delta/tables/
│       └── classic/                        # NEW subdirectory
│           ├── DeltaTable.scala           # extends tables.DeltaTable
│           ├── DeltaTableBuilder.scala    # extends tables.DeltaTableBuilder
│           ├── DeltaColumnBuilder.scala   # extends tables.DeltaColumnBuilder
│           ├── DeltaMergeBuilder.scala    # extends tables.DeltaMergeBuilder
│           ├── DeltaOptimizeBuilder.scala # extends tables.DeltaOptimizeBuilder
│           └── execution/                  # Implementation helpers
│               ├── DeltaTableOperations.scala
│               └── DeltaConvert.scala
│
└── spark-connect/client/                   # Connect implementation
    └── src/main/scala-spark-master/io/delta/tables/
        └── connect/                        # RENAMED from io/delta/connect/tables
            ├── DeltaTable.scala           # extends tables.DeltaTable
            ├── DeltaTableBuilder.scala    # extends tables.DeltaTableBuilder
            ├── DeltaColumnBuilder.scala   # extends tables.DeltaColumnBuilder
            ├── DeltaMergeBuilder.scala    # extends tables.DeltaMergeBuilder
            ├── DeltaOptimizeBuilder.scala # extends tables.DeltaOptimizeBuilder
            └── execution/
                └── DeltaTableBuilderOptions.scala
```

---

## Phase 1: Create the Public API Module

### 1.1 Create New API Module Structure

Create a new SBT module `spark-api` or add to existing module structure:

```
spark-api/
├── build.sbt (or modify root build.sbt)
└── src/main/scala/io/delta/tables/
```

### 1.2 Documentation Strategy

All Scaladoc documentation lives in the API layer. Implementation classes inherit documentation
using the `@inheritdoc` tag, following Spark's pattern.

**Principles:**
- API classes contain complete Scaladoc with `@param`, `@return`, `@since`, `@example` tags
- Implementation classes use `/** @inheritdoc */` to inherit documentation
- Implementation-specific behavior can be added after `@inheritdoc` if needed
- No duplicate documentation between API and implementations

**Example pattern in implementations:**
```scala
/** @inheritdoc */
override def vacuum(): DataFrame = { ... }

/** @inheritdoc */
override def delete(condition: Column): Unit = { ... }
```

### 1.3 Define Abstract DeltaTable

Create `spark-api/src/main/scala/io/delta/tables/DeltaTable.scala`:

```scala
package io.delta.tables

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

/**
 * Main class for programmatically interacting with Delta tables.
 * You can create instances using the static methods in the companion object.
 *
 * @example {{{
 *   // Get a DeltaTable instance for a path
 *   val deltaTable = DeltaTable.forPath(spark, "/path/to/table")
 *
 *   // Get a DeltaTable instance for a table name
 *   val deltaTable = DeltaTable.forName(spark, "my_table")
 * }}}
 *
 * @since 0.3.0
 */
abstract class DeltaTable extends Serializable {

  /**
   * The SparkSession associated with this DeltaTable.
   * @since 0.3.0
   */
  def sparkSession: SparkSession

  /**
   * Apply an alias to the DeltaTable. This is similar to [[Dataset.as(alias)]] or
   * SQL `auto AS alias`.
   *
   * @param alias the table alias
   * @return aliased DeltaTable as a DataFrame
   * @since 0.3.0
   */
  def as(alias: String): DataFrame

  /**
   * Get a DataFrame representation of this Delta table.
   * @since 0.3.0
   */
  def toDF: DataFrame

  // === Query Operations ===

  /**
   * Get the information of the latest `limit` commits on this table as a Spark DataFrame.
   * The information is in reverse chronological order.
   *
   * @param limit the number of previous commits to return
   * @return a DataFrame containing the commit history
   * @since 0.3.0
   */
  def history(limit: Int): DataFrame

  /**
   * Get the information of all commits on this table as a Spark DataFrame.
   * The information is in reverse chronological order.
   *
   * @return a DataFrame containing the full commit history
   * @since 0.3.0
   */
  def history(): DataFrame

  /**
   * Get the details of a Delta table such as the format, name, and size.
   *
   * @return a DataFrame containing the table details
   * @since 0.4.0
   */
  def detail(): DataFrame

  // === Write Operations ===

  /**
   * Delete data from the table that matches the given condition.
   *
   * @param condition the condition to match for deletion, as a SQL expression string
   * @since 0.3.0
   */
  def delete(condition: String): Unit

  /**
   * Delete data from the table that matches the given condition.
   *
   * @param condition the condition to match for deletion, as a Column
   * @since 0.3.0
   */
  def delete(condition: Column): Unit

  /**
   * Delete all data from the table.
   * @since 0.3.0
   */
  def delete(): Unit

  /**
   * Update rows in the table based on the given condition and update expressions.
   *
   * @param condition the condition to match for update
   * @param set a map from column name to update expression (as Column)
   * @since 0.3.0
   */
  def update(condition: Column, set: Map[String, Column]): Unit

  /**
   * Update all rows in the table with the given update expressions.
   *
   * @param set a map from column name to update expression (as Column)
   * @since 0.3.0
   */
  def update(set: Map[String, Column]): Unit

  /**
   * Update rows in the table based on the given condition and update expressions.
   *
   * @param condition the condition to match for update, as a SQL expression string
   * @param set a map from column name to update SQL expression string
   * @since 0.3.0
   */
  def updateExpr(condition: String, set: Map[String, String]): Unit

  /**
   * Update all rows in the table with the given update expressions.
   *
   * @param set a map from column name to update SQL expression string
   * @since 0.3.0
   */
  def updateExpr(set: Map[String, String]): Unit

  /**
   * Merge data from the source DataFrame based on the given merge condition.
   *
   * @param source the source DataFrame to merge from
   * @param condition the merge condition, as a SQL expression string
   * @return a [[DeltaMergeBuilder]] to specify the merge actions
   * @since 0.3.0
   */
  def merge(source: DataFrame, condition: String): DeltaMergeBuilder

  /**
   * Merge data from the source DataFrame based on the given merge condition.
   *
   * @param source the source DataFrame to merge from
   * @param condition the merge condition, as a Column
   * @return a [[DeltaMergeBuilder]] to specify the merge actions
   * @since 0.3.0
   */
  def merge(source: DataFrame, condition: Column): DeltaMergeBuilder

  // === Maintenance Operations ===

  /**
   * Recursively delete files and directories in the table that are not needed by the table
   * for maintaining older versions up to the given retention threshold.
   *
   * @param retentionHours the retention threshold in hours. Files older than this will be removed.
   * @return a DataFrame with statistics about the vacuum operation
   * @since 0.3.0
   */
  def vacuum(retentionHours: Double): DataFrame

  /**
   * Recursively delete files and directories in the table that are not needed by the table
   * for maintaining older versions up to the default retention threshold.
   *
   * @return a DataFrame with statistics about the vacuum operation
   * @since 0.3.0
   */
  def vacuum(): DataFrame

  /**
   * Optimize the data layout of the table.
   *
   * @return a [[DeltaOptimizeBuilder]] to specify optimization parameters
   * @since 2.0.0
   */
  def optimize(): DeltaOptimizeBuilder

  /**
   * Restore the table to an older version of the table specified by version number.
   *
   * @param version the version number to restore to
   * @return a DataFrame with statistics about the restore operation
   * @since 1.2.0
   */
  def restoreToVersion(version: Long): DataFrame

  /**
   * Restore the table to an older version of the table specified by a timestamp.
   *
   * @param timestamp the timestamp to restore to, in format yyyy-MM-dd HH:mm:ss
   * @return a DataFrame with statistics about the restore operation
   * @since 1.2.0
   */
  def restoreToTimestamp(timestamp: String): DataFrame

  /**
   * Generate a manifest file for the table.
   *
   * @param mode the mode for manifest generation ("symlink_format_manifest")
   * @since 0.5.0
   */
  def generate(mode: String): Unit

  // === Protocol Operations ===

  /**
   * Upgrade the protocol version of the table to support new features.
   *
   * @param readerVersion the minimum reader version to upgrade to
   * @param writerVersion the minimum writer version to upgrade to
   * @since 0.8.0
   */
  def upgradeTableProtocol(readerVersion: Int, writerVersion: Int): Unit

  /**
   * Add support for a table feature.
   *
   * @param featureName the name of the feature to enable
   * @since 3.1.0
   */
  def addFeatureSupport(featureName: String): Unit

  /**
   * Drop support for a table feature.
   *
   * @param featureName the name of the feature to drop
   * @since 3.1.0
   */
  def dropFeatureSupport(featureName: String): Unit

  /**
   * Drop support for a table feature, optionally truncating history.
   *
   * @param featureName the name of the feature to drop
   * @param truncateHistory if true, truncate history to allow dropping the feature
   * @since 3.1.0
   */
  def dropFeatureSupport(featureName: String, truncateHistory: Boolean): Unit

  // === Clone Operations ===

  /**
   * Clone this table to a new location.
   *
   * @param target the target path for the cloned table
   * @return a [[DeltaTableBuilder]] to specify clone options
   * @since 1.0.0
   */
  def clone(target: String): DeltaTableBuilder

  /**
   * Clone this table to a new location.
   *
   * @param target the target path for the cloned table
   * @param isShallow if true, create a shallow clone (references source files)
   * @return a [[DeltaTableBuilder]] to specify clone options
   * @since 1.0.0
   */
  def clone(target: String, isShallow: Boolean): DeltaTableBuilder
}

/**
 * Companion object with static factory methods.
 */
object DeltaTable extends DeltaTableCompanion {
  // Delegate to implementation-specific companion
}

/**
 * Abstract companion for implementation selection.
 */
abstract class DeltaTableCompanion {
  type Table <: DeltaTable

  def forPath(sparkSession: SparkSession, path: String): Table
  def forPath(sparkSession: SparkSession, path: String,
              hadoopConf: Map[String, String]): Table
  def forName(sparkSession: SparkSession, tableName: String): Table

  def isDeltaTable(sparkSession: SparkSession, identifier: String): Boolean

  def createIfNotExists(sparkSession: SparkSession): DeltaTableBuilder
  def create(sparkSession: SparkSession): DeltaTableBuilder
  def replace(sparkSession: SparkSession): DeltaTableBuilder
  def createOrReplace(sparkSession: SparkSession): DeltaTableBuilder

  def columnBuilder(sparkSession: SparkSession, colName: String): DeltaColumnBuilder
}
```

### 1.3 Define Abstract DeltaTableBuilder

Create `spark-api/src/main/scala/io/delta/tables/DeltaTableBuilder.scala`:

```scala
package io.delta.tables

import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Builder to specify how to create/replace a Delta table.
 */
abstract class DeltaTableBuilder {

  /** Set the table identifier (name). */
  def tableName(identifier: String): this.type

  /** Add a column. */
  def addColumn(colName: String, dataType: String): this.type
  def addColumn(colName: String, dataType: DataType): this.type
  def addColumn(colName: String, dataType: String, nullable: Boolean): this.type
  def addColumn(colName: String, dataType: DataType, nullable: Boolean): this.type
  def addColumn(col: StructField): this.type
  def addColumn(col: DeltaColumnBuilder): this.type
  def addColumns(cols: StructField*): this.type

  /** Set comment on the table. */
  def comment(comment: String): this.type

  /** Set location of the table. */
  def location(location: String): this.type

  /** Add a table property. */
  def property(key: String, value: String): this.type

  /** Partition the table by columns. */
  def partitionedBy(colNames: String*): this.type
  def partitionedBy(col: Column, cols: Column*): this.type

  /** Cluster the table by columns (liquid clustering). */
  def clusterBy(colNames: String*): this.type

  /** Execute the builder to create/replace the table. */
  def execute(): DeltaTable
}

/** Builder options - shared between implementations */
sealed trait DeltaTableBuilderOptions
case class CreateTableOptions(ifNotExists: Boolean) extends DeltaTableBuilderOptions
case class ReplaceTableOptions(orCreate: Boolean) extends DeltaTableBuilderOptions
```

### 1.4 Define Abstract DeltaColumnBuilder

Create `spark-api/src/main/scala/io/delta/tables/DeltaColumnBuilder.scala`:

```scala
package io.delta.tables

import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Builder to specify a column in a Delta table.
 */
abstract class DeltaColumnBuilder {

  /** Column name (set at construction) */
  def colName: String

  /** Set the data type. */
  def dataType(dataType: String): this.type
  def dataType(dataType: DataType): this.type

  /** Set whether the column is nullable. */
  def nullable(nullable: Boolean): this.type

  /** Set a generated expression. */
  def generatedAlwaysAs(expr: String): this.type

  /** Set a comment on the column. */
  def comment(comment: String): this.type

  /** Set identity column properties. */
  def identity(): this.type
  def identity(start: Long, step: Long): this.type
  def identity(start: Long, step: Long, allowExplicitInsert: Boolean): this.type

  /** Build the StructField. */
  def build(): StructField
}
```

### 1.5 Define Abstract DeltaMergeBuilder

Create `spark-api/src/main/scala/io/delta/tables/DeltaMergeBuilder.scala`:

```scala
package io.delta.tables

import org.apache.spark.sql.{Column, DataFrame}

/**
 * Builder for merge operations.
 */
abstract class DeltaMergeBuilder {

  /** Specify matched clause. */
  def whenMatched(): DeltaMergeMatchedActionBuilder
  def whenMatched(condition: String): DeltaMergeMatchedActionBuilder
  def whenMatched(condition: Column): DeltaMergeMatchedActionBuilder

  /** Specify not matched clause. */
  def whenNotMatched(): DeltaMergeNotMatchedActionBuilder
  def whenNotMatched(condition: String): DeltaMergeNotMatchedActionBuilder
  def whenNotMatched(condition: Column): DeltaMergeNotMatchedActionBuilder

  /** Specify not matched by source clause. */
  def whenNotMatchedBySource(): DeltaMergeNotMatchedBySourceActionBuilder
  def whenNotMatchedBySource(condition: String): DeltaMergeNotMatchedBySourceActionBuilder
  def whenNotMatchedBySource(condition: Column): DeltaMergeNotMatchedBySourceActionBuilder

  /** Execute the merge operation. */
  def execute(): Unit

  /** Execute with schema evolution. */
  def withSchemaEvolution(): DeltaMergeBuilder
}

abstract class DeltaMergeMatchedActionBuilder {
  def update(set: Map[String, Column]): DeltaMergeBuilder
  def updateExpr(set: Map[String, String]): DeltaMergeBuilder
  def updateAll(): DeltaMergeBuilder
  def delete(): DeltaMergeBuilder
}

abstract class DeltaMergeNotMatchedActionBuilder {
  def insert(values: Map[String, Column]): DeltaMergeBuilder
  def insertExpr(values: Map[String, String]): DeltaMergeBuilder
  def insertAll(): DeltaMergeBuilder
}

abstract class DeltaMergeNotMatchedBySourceActionBuilder {
  def update(set: Map[String, Column]): DeltaMergeBuilder
  def updateExpr(set: Map[String, String]): DeltaMergeBuilder
  def delete(): DeltaMergeBuilder
}
```

### 1.6 Define Abstract DeltaOptimizeBuilder

Create `spark-api/src/main/scala/io/delta/tables/DeltaOptimizeBuilder.scala`:

```scala
package io.delta.tables

import org.apache.spark.sql.DataFrame

/**
 * Builder for optimize operations.
 */
abstract class DeltaOptimizeBuilder {

  /** Filter partitions to optimize. */
  def where(partitionFilter: String): this.type

  /** Execute without Z-ORDER. */
  def executeCompaction(): DataFrame

  /** Execute with Z-ORDER on specified columns. */
  def executeZOrderBy(columns: String*): DataFrame
  def executeZOrderBy(columns: Seq[String]): DataFrame
}
```

### 1.7 Package Object for Type Aliases

Create `spark-api/src/main/scala/io/delta/tables/package.scala`:

```scala
package io.delta

package object tables {
  // Mode selection (similar to Spark's .classic() and .connect())
  private[tables] var defaultCompanion: DeltaTableCompanion = _

  def setDefaultImplementation(companion: DeltaTableCompanion): Unit = {
    defaultCompanion = companion
  }
}
```

---

## Phase 2: Refactor Classic Implementation

### 2.1 Move Classic Code to Subpackage

Move existing files from `spark/src/main/scala/io/delta/tables/` to
`spark/src/main/scala/io/delta/tables/classic/`:

```bash
# Files to move:
# DeltaTable.scala -> classic/DeltaTable.scala
# DeltaTableBuilder.scala -> classic/DeltaTableBuilder.scala
# DeltaColumnBuilder.scala -> classic/DeltaColumnBuilder.scala
# DeltaMergeBuilder.scala -> classic/DeltaMergeBuilder.scala
# DeltaOptimizeBuilder.scala -> classic/DeltaOptimizeBuilder.scala
# execution/* -> classic/execution/*
```

### 2.2 Update Classic DeltaTable to Extend API

Modify `spark/src/main/scala/io/delta/tables/classic/DeltaTable.scala`:

```scala
package io.delta.tables.classic

import io.delta.tables.{DeltaTable, DeltaTableCompanion}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.delta.DeltaTableV2

/**
 * Classic implementation of DeltaTable for local Spark execution.
 */
class DeltaTable private[tables](
    @transient private val _df: Dataset[Row],
    @transient private val table: DeltaTableV2)
  extends DeltaTable
  with DeltaTableOperations
  with Serializable {

  /** @inheritdoc */
  override def sparkSession: SparkSession = _df.sparkSession

  /** @inheritdoc */
  override def toDF: DataFrame = _df

  /** @inheritdoc */
  override def as(alias: String): DataFrame = _df.as(alias)

  /** @inheritdoc */
  override def history(): DataFrame = executeHistory(table, None)

  /** @inheritdoc */
  override def history(limit: Int): DataFrame = executeHistory(table, Some(limit))

  /** @inheritdoc */
  override def delete(): Unit = executeDelete(None)

  /** @inheritdoc */
  override def delete(condition: String): Unit = delete(functions.expr(condition))

  /** @inheritdoc */
  override def delete(condition: Column): Unit = executeDelete(Some(condition.expr))

  // ... rest of implementations with @inheritdoc
}

object DeltaTable extends DeltaTableCompanion {
  override type Table = DeltaTable

  override def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    forPath(sparkSession, path, Map.empty)
  }

  override def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: Map[String, String]): DeltaTable = {
    val hdpPath = new Path(path)
    if (DeltaTableUtils.isDeltaTable(sparkSession, hdpPath, hadoopConf)) {
      new DeltaTable(
        sparkSession.read.format("delta").options(hadoopConf).load(path),
        DeltaTableV2(spark = sparkSession, path = hdpPath, options = hadoopConf))
    } else {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(path = Some(path)))
    }
  }

  override def forName(sparkSession: SparkSession, tableName: String): DeltaTable = {
    // ... implementation
  }

  // ... rest of companion methods
}
```

### 2.3 Update Classic Builders

Similarly update all builder classes to extend their API counterparts:

```scala
// DeltaTableBuilder.scala
package io.delta.tables.classic

class DeltaTableBuilder private[tables](
    spark: SparkSession,
    builderOption: DeltaTableBuilderOptions)
  extends io.delta.tables.DeltaTableBuilder {

  // ... implementation
}

// DeltaColumnBuilder.scala
package io.delta.tables.classic

class DeltaColumnBuilder private[tables](
    private val spark: SparkSession,
    override val colName: String)
  extends io.delta.tables.DeltaColumnBuilder {

  // ... implementation
}

// DeltaMergeBuilder.scala
package io.delta.tables.classic

class DeltaMergeBuilder private[tables](...)
  extends io.delta.tables.DeltaMergeBuilder {

  // ... implementation
}

// DeltaOptimizeBuilder.scala
package io.delta.tables.classic

class DeltaOptimizeBuilder private[tables](table: DeltaTableV2)
  extends io.delta.tables.DeltaOptimizeBuilder {

  // ... implementation
}
```

### 2.4 Create Classic Package Object

Create `spark/src/main/scala/io/delta/tables/classic/package.scala`:

```scala
package io.delta.tables

package object classic {
  // Register classic implementation as default when this package is loaded
  tables.setDefaultImplementation(DeltaTable)
}
```

---

## Phase 3: Refactor Connect Implementation

### 3.1 Move Connect Code to New Package

Move files from:
`spark-connect/client/src/main/scala-spark-master/io/delta/connect/tables/`

To:
`spark-connect/client/src/main/scala-spark-master/io/delta/tables/connect/`

### 3.2 Update Connect DeltaTable to Extend API

Modify `spark-connect/client/src/main/scala-spark-master/io/delta/tables/connect/DeltaTable.scala`:

```scala
package io.delta.tables.connect

import io.delta.tables.{DeltaTable, DeltaTableCompanion}
import io.delta.connect.proto
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

/**
 * Connect implementation of DeltaTable for remote Spark Connect execution.
 */
class DeltaTable private[tables](
    private val df: Dataset[Row],
    private val table: proto.DeltaTable)
  extends DeltaTable
  with Serializable {

  /** @inheritdoc */
  override def sparkSession: SparkSession = df.sparkSession

  /** @inheritdoc */
  override def toDF: DataFrame = df

  /** @inheritdoc */
  override def as(alias: String): DataFrame = df.as(alias)

  /** @inheritdoc */
  override def history(): DataFrame = executeHistory(None)

  /** @inheritdoc */
  override def history(limit: Int): DataFrame = executeHistory(Some(limit))

  private def executeHistory(limit: Option[Int]): DataFrame = {
    val describeHistory = proto.DescribeHistory
      .newBuilder()
      .setTable(table)
    limit.foreach(describeHistory.setLimit)
    // ... build relation and execute
  }

  /** @inheritdoc */
  override def delete(): Unit = executeDelete(None)

  /** @inheritdoc */
  override def delete(condition: String): Unit = delete(functions.expr(condition))

  /** @inheritdoc */
  override def delete(condition: Column): Unit = executeDelete(Some(condition))

  private def executeDelete(condition: Option[Column]): Unit = {
    val delete = proto.DeleteFromTable.newBuilder().setTarget(df.plan.getRoot)
    condition.foreach(c => delete.setCondition(toExpr(c)))
    // ... build relation and execute
  }

  // ... rest of implementations with @inheritdoc
}

object DeltaTable extends DeltaTableCompanion {
  override type Table = DeltaTable

  override def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    forPath(sparkSession, path, Map.empty)
  }

  override def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: Map[String, String]): DeltaTable = {
    val table = proto.DeltaTable
      .newBuilder()
      .setPath(
        proto.DeltaTable.Path.newBuilder()
          .setPath(path)
          .putAllHadoopConf(hadoopConf.asJava))
      .build()
    forTable(sparkSession, table)
  }

  // ... rest of companion methods
}
```

### 3.3 Update Connect Builders

Similarly update all builder classes:

```scala
// DeltaTableBuilder.scala
package io.delta.tables.connect

class DeltaTableBuilder private[tables](
    spark: SparkSession,
    builderOption: DeltaTableBuilderOptions)
  extends io.delta.tables.DeltaTableBuilder {

  // ... protobuf-based implementation
}

// DeltaColumnBuilder.scala
package io.delta.tables.connect

class DeltaColumnBuilder private[tables](override val colName: String)
  extends io.delta.tables.DeltaColumnBuilder {

  // ... implementation (no SparkSession needed)
}

// etc.
```

### 3.4 Create Connect Package Object

Create `spark-connect/client/src/main/scala-spark-master/io/delta/tables/connect/package.scala`:

```scala
package io.delta.tables

package object connect {
  // Register connect implementation as default when this package is loaded
  tables.setDefaultImplementation(DeltaTable)
}
```

---

## Phase 4: Create Unified Entry Point

### 4.1 Update Main DeltaTable Companion Object

In `spark-api/src/main/scala/io/delta/tables/DeltaTable.scala`, update the companion:

```scala
object DeltaTable {

  /** Get the current implementation companion. */
  private def companion: DeltaTableCompanion = {
    if (tables.defaultCompanion == null) {
      // Try to load classic by default
      try {
        Class.forName("io.delta.tables.classic.DeltaTable$")
      } catch {
        case _: ClassNotFoundException =>
          try {
            Class.forName("io.delta.tables.connect.DeltaTable$")
          } catch {
            case _: ClassNotFoundException =>
              throw new IllegalStateException(
                "No Delta implementation found. Add delta-spark or delta-connect to classpath.")
          }
      }
    }
    tables.defaultCompanion
  }

  def forPath(sparkSession: SparkSession, path: String): DeltaTable =
    companion.forPath(sparkSession, path)

  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: Map[String, String]): DeltaTable =
    companion.forPath(sparkSession, path, hadoopConf)

  def forName(sparkSession: SparkSession, tableName: String): DeltaTable =
    companion.forName(sparkSession, tableName)

  def isDeltaTable(sparkSession: SparkSession, identifier: String): Boolean =
    companion.isDeltaTable(sparkSession, identifier)

  def create(sparkSession: SparkSession): DeltaTableBuilder =
    companion.create(sparkSession)

  def createIfNotExists(sparkSession: SparkSession): DeltaTableBuilder =
    companion.createIfNotExists(sparkSession)

  def replace(sparkSession: SparkSession): DeltaTableBuilder =
    companion.replace(sparkSession)

  def createOrReplace(sparkSession: SparkSession): DeltaTableBuilder =
    companion.createOrReplace(sparkSession)

  def columnBuilder(sparkSession: SparkSession, colName: String): DeltaColumnBuilder =
    companion.columnBuilder(sparkSession, colName)
}
```

---

## Phase 5: Build System Updates

### 5.1 SBT Configuration

Update `build.sbt` to add the new API module:

```scala
lazy val sparkApi = (project in file("spark-api"))
  .settings(
    name := "delta-spark-api",
    // Minimal dependencies - only Spark SQL API types
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql-api" % sparkVersion % "provided"
    )
  )

lazy val deltaSpark = (project in file("spark"))
  .dependsOn(sparkApi)
  .settings(
    name := "delta-spark",
    // Full Spark dependencies
  )

lazy val deltaConnectClient = (project in file("spark-connect/client"))
  .dependsOn(sparkApi)
  .settings(
    name := "delta-connect-client",
    // Spark Connect client dependencies
  )
```

### 5.2 Maven/POM Updates (if applicable)

If using Maven, create corresponding `pom.xml` for the API module:

```xml
<project>
  <artifactId>delta-spark-api_${scala.binary.version}</artifactId>

  <dependencies>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql-api_${scala.binary.version}</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
</project>
```

---

## Phase 6: Migration and Testing

### 6.1 Backward Compatibility

To maintain backward compatibility:

1. Keep `io.delta.tables` as the primary user-facing package
2. Users importing `io.delta.tables._` will get the API classes
3. The correct implementation is selected based on classpath:
   - If `delta-spark` (depends on `delta-spark-api`) is on classpath → Classic implementation
   - If `delta-connect-client` (depends on `delta-spark-api`) is on classpath → Connect implementation

### 6.2 Deprecation Path for Connect Users

Current Connect users import from `io.delta.connect.tables`. Provide deprecation:

```scala
// In spark-connect/client, create forwarding package
package io.delta.connect

package object tables {
  @deprecated("Use io.delta.tables.connect instead", "4.0.0")
  type DeltaTable = io.delta.tables.connect.DeltaTable

  @deprecated("Use io.delta.tables.connect instead", "4.0.0")
  val DeltaTable = io.delta.tables.connect.DeltaTable

  // ... other type aliases
}
```

### 6.3 Test Strategy

1. **API Tests**: Create tests that only use API types (no implementation details)
2. **Classic Tests**: Existing tests in `spark/` module continue to work
3. **Connect Tests**: Existing tests in `spark-connect/` module need modifications (see below)
4. **Cross-implementation Tests**: Tests that verify both implementations have same behavior

### 6.4 Delta Connect Test Modifications

The existing Delta Connect tests are located in:
```
spark-connect/client/src/test/scala-spark-master/io/delta/connect/tables/
├── DeltaTableSuite.scala
├── DeltaTableBuilderSuite.scala
├── DeltaMergeBuilderSuite.scala
├── DeltaQueryTest.scala
└── RemoteSparkSession.scala
```

#### 6.4.1 Directory Structure Changes

Move test files from:
```
spark-connect/client/src/test/scala-spark-master/io/delta/connect/tables/
```

To:
```
spark-connect/client/src/test/scala-spark-master/io/delta/tables/connect/
```

#### 6.4.2 Package Declaration Changes

**Before:**
```scala
// DeltaTableSuite.scala (current)
package io.delta.tables  // Note: file is in io/delta/connect/tables/ but uses this package

import org.apache.spark.sql.test.DeltaQueryTest

class DeltaTableSuite extends DeltaQueryTest with RemoteSparkSession {
  // Uses DeltaTable.forPath(spark, path) which resolves to Connect impl
}
```

**After:**
```scala
// DeltaTableSuite.scala (refactored)
package io.delta.tables.connect

import io.delta.tables.DeltaTable  // Import API
import org.apache.spark.sql.test.DeltaQueryTest

class DeltaTableSuite extends DeltaQueryTest with RemoteSparkSession {
  // Still uses DeltaTable.forPath(spark, path)
  // But now DeltaTable is the API, and Connect impl is auto-selected
}
```

#### 6.4.3 Import Changes for Each Test File

**DeltaTableSuite.scala:**
```scala
// Before
package io.delta.tables

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit}

class DeltaTableSuite extends DeltaQueryTest with RemoteSparkSession {
  test("vacuum") {
    // Uses fully qualified: io.delta.tables.DeltaTable.forPath(...)
    val table = io.delta.tables.DeltaTable.forPath(spark, dir.getAbsolutePath)
  }
}

// After
package io.delta.tables.connect

import io.delta.tables.DeltaTable  // Import API class
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit}

class DeltaTableSuite extends DeltaQueryTest with RemoteSparkSession {
  test("vacuum") {
    // Now just use imported DeltaTable
    val table = DeltaTable.forPath(spark, dir.getAbsolutePath)
  }
}
```

**DeltaMergeBuilderSuite.scala:**
```scala
// Before
package io.delta.tables

import org.apache.spark.sql.Row

class DeltaMergeBuilderSuite extends DeltaQueryTest with RemoteSparkSession {
  test("merge") {
    val deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.merge(source, "key = k")
      .whenMatched().updateExpr(Map("value" -> "value + v"))
      .execute()
  }
}

// After
package io.delta.tables.connect

import io.delta.tables.{DeltaTable, DeltaMergeBuilder}  // Import API classes
import org.apache.spark.sql.Row

class DeltaMergeBuilderSuite extends DeltaQueryTest with RemoteSparkSession {
  test("merge") {
    val deltaTable = DeltaTable.forPath(spark, path)
    // DeltaMergeBuilder returned is API type, impl is Connect
    deltaTable.merge(source, "key = k")
      .whenMatched().updateExpr(Map("value" -> "value + v"))
      .execute()
  }
}
```

**DeltaTableBuilderSuite.scala:**
```scala
// Before
package io.delta.tables

class DeltaTableBuilderSuite extends DeltaQueryTest with RemoteSparkSession {
  test("create table") {
    DeltaTable.create(spark)
      .tableName("testTable")
      .addColumn("id", "INT")
      .execute()
  }
}

// After
package io.delta.tables.connect

import io.delta.tables.DeltaTable  // Import API class

class DeltaTableBuilderSuite extends DeltaQueryTest with RemoteSparkSession {
  test("create table") {
    DeltaTable.create(spark)
      .tableName("testTable")
      .addColumn("id", "INT")
      .execute()
  }
}
```

#### 6.4.4 Test Infrastructure Changes

**DeltaQueryTest.scala** - No changes needed, stays in `org.apache.spark.sql.test` package.

**RemoteSparkSession.scala:**
```scala
// Before
package io.delta.tables

trait RemoteSparkSession { ... }

// After
package io.delta.tables.connect

trait RemoteSparkSession { ... }
```

#### 6.4.5 Type Assertions in Tests

If tests need to verify they're using the Connect implementation specifically:

```scala
package io.delta.tables.connect

import io.delta.tables.DeltaTable
import io.delta.tables.connect.{DeltaTable => DeltaTableConnect}

class DeltaTableImplSuite extends DeltaQueryTest with RemoteSparkSession {
  test("verify connect implementation is used") {
    val table = DeltaTable.forPath(spark, path)
    // Verify the runtime type is the Connect implementation
    assert(table.isInstanceOf[DeltaTableConnect],
      "Expected Connect implementation but got: " + table.getClass.getName)
  }
}
```

#### 6.4.6 Handling Tests That Access Implementation Details

Some tests may access Connect-specific internals. These should be updated to:

1. **Keep in Connect-specific test package** if they test Connect-only behavior
2. **Use pattern matching** to access implementation details when needed:

```scala
package io.delta.tables.connect

import io.delta.tables.DeltaTable
import io.delta.tables.connect.{DeltaTable => DeltaTableConnect}

class DeltaTableConnectInternalsSuite extends DeltaQueryTest with RemoteSparkSession {
  test("access connect-specific internals") {
    val table = DeltaTable.forPath(spark, path)

    // Pattern match to access Connect-specific fields
    table match {
      case connectTable: DeltaTableConnect =>
        // Access Connect-specific internals for testing
        val protoTable = connectTable.table  // proto.DeltaTable
        assert(protoTable.hasPath)
      case other =>
        fail(s"Expected DeltaTableConnect but got ${other.getClass}")
    }
  }
}
```

#### 6.4.7 Summary of Changes Per File

| File | Package Change | Import Changes | Other Changes |
|------|----------------|----------------|---------------|
| `DeltaTableSuite.scala` | `io.delta.tables` → `io.delta.tables.connect` | Add `import io.delta.tables.DeltaTable` | Remove `io.delta.tables.` prefix from code |
| `DeltaMergeBuilderSuite.scala` | `io.delta.tables` → `io.delta.tables.connect` | Add `import io.delta.tables.DeltaTable` | None |
| `DeltaTableBuilderSuite.scala` | `io.delta.tables` → `io.delta.tables.connect` | Add `import io.delta.tables.DeltaTable` | None |
| `RemoteSparkSession.scala` | `io.delta.tables` → `io.delta.tables.connect` | None | None |
| `DeltaQueryTest.scala` | No change (`org.apache.spark.sql.test`) | None | None |

#### 6.4.8 Build Configuration Updates

Update test source directories in `build.sbt`:

```scala
lazy val deltaConnectClient = (project in file("spark-connect/client"))
  .dependsOn(sparkApi)
  .settings(
    name := "delta-connect-client",
    // Test sources now in new location
    Test / unmanagedSourceDirectories +=
      baseDirectory.value / "src/test/scala-spark-master/io/delta/tables/connect"
  )
```

#### 6.4.9 Ensuring Tests Use Connect Implementation

Since tests are in the `delta-connect-client` module, they will automatically use the Connect
implementation because:

1. The module depends on `sparkApi` (API classes)
2. The module contains `io.delta.tables.connect.DeltaTable` (Connect implementation)
3. The Connect implementation registers itself as the default when loaded
4. Classic implementation is NOT on the test classpath

If you need to explicitly verify Connect is being used in tests:

```scala
import io.delta.tables

class ConnectImplementationTest extends DeltaQueryTest with RemoteSparkSession {
  test("connect implementation is active") {
    assert(
      tables.defaultCompanion.getClass.getName.contains("connect"),
      "Expected Connect implementation to be registered"
    )
  }
}
```

---

## Phase 7: Documentation Updates

### 7.1 User Documentation

Update documentation to explain the new package structure:

```markdown
# Delta Lake API

## Basic Usage

import io.delta.tables._

// Works with both Classic and Connect - implementation auto-selected based on classpath
val deltaTable = DeltaTable.forPath(spark, "/path/to/table")

## Explicit Implementation Selection

// When you need a specific implementation, import it with a rename:
import io.delta.tables.DeltaTable  // API (always use this as the primary type)
import io.delta.tables.classic.{DeltaTable => DeltaTableClassic}  // Classic impl
import io.delta.tables.connect.{DeltaTable => DeltaTableConnect}  // Connect impl

// Use API type for variables, specific impl for instantiation if needed
val table: DeltaTable = DeltaTableClassic.forPath(spark, "/path/to/table")
```

### 7.2 Migration Guide

Create migration guide for users upgrading from previous versions.

---

## Implementation Order

1. **Phase 1**: Create API module with abstract definitions
2. **Phase 2**: Refactor Classic implementation to extend API
3. **Phase 3**: Refactor Connect implementation to extend API
4. **Phase 4**: Create unified entry point
5. **Phase 5**: Update build system
6. **Phase 6**: Migration testing and backward compatibility
7. **Phase 7**: Documentation updates

---

## Risk Mitigation

### Binary Compatibility
- The API module should be stable and avoid breaking changes
- Consider using MiMa (Migration Manager) to check binary compatibility

### Runtime Conflicts
- Ensure only one implementation is on the classpath at runtime
- Add clear error messages when both or neither are present

### Performance
- The API layer should be thin with no runtime overhead
- Avoid reflection in hot paths

---

## Alternative Approaches Considered

### 1. Trait-based API
Instead of abstract classes, use traits. Rejected because:
- Traits can't have constructor parameters
- Abstract classes align better with Spark's pattern

### 2. Separate packages without shared API
Keep Classic and Connect completely separate. Rejected because:
- Code duplication
- No compile-time contract enforcement
- Harder to maintain feature parity

### 3. Single implementation with runtime dispatch
One class that dispatches to Classic or Connect. Rejected because:
- More complex runtime logic
- Harder to test
- Not aligned with Spark's approach
