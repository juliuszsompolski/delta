# Delta Connect API Refactoring - Implementation Progress

## Status: In Progress - Phase 4 (Cleanup/Testing)

## Phase 1: Create the Public API Module - COMPLETED

### 1.1 Create Module Structure - DONE
- [x] Created `spark-api/src/main/scala/io/delta/tables/` directory structure

### 1.2 Define Abstract Classes - DONE
- [x] `DeltaTable.scala` - Abstract class with all instance methods
- [x] `DeltaTableBuilder.scala` - Abstract builder class + `DeltaTableBuilderOptions` sealed trait
- [x] `DeltaColumnBuilder.scala` - Abstract builder for columns
- [x] `DeltaMergeBuilder.scala` - Abstract merge builder + action builder classes
- [x] `DeltaOptimizeBuilder.scala` - Abstract optimize builder
- [x] `package.scala` - Package object + `DeltaTableCompanion` abstract class + `DeltaTable` companion object

## Phase 2: Refactor Classic Implementation - COMPLETED

### 2.1 Created Classic Implementation Files
- [x] `classic/DeltaTable.scala` - Extends API DeltaTable, includes companion object extending DeltaTableCompanion
- [x] `classic/DeltaTableBuilder.scala` - Extends API DeltaTableBuilder
- [x] `classic/DeltaColumnBuilder.scala` - Extends API DeltaColumnBuilder
- [x] `classic/DeltaMergeBuilder.scala` - Extends API merge builders (all 4 classes)
- [x] `classic/DeltaOptimizeBuilder.scala` - Extends API DeltaOptimizeBuilder
- [x] `classic/package.scala` - Registers classic as default implementation

### 2.2 Updated Execution Package
- [x] `classic/execution/DeltaConvert.scala` - Updated package and imports
- [x] `classic/execution/DeltaTableOperations.scala` - Updated package and self-type
- [x] `classic/execution/VacuumTableCommand.scala` - Updated package
- [x] Removed `DeltaTableBuilderOptions.scala` (now in API module)

## Phase 3: Refactor Connect Implementation - COMPLETED

### 3.1 Created Connect Implementation Files
- [x] `connect/DeltaTable.scala` - Extends API DeltaTable, companion extends DeltaTableCompanion
- [x] `connect/DeltaTableBuilder.scala` - Extends API DeltaTableBuilder
- [x] `connect/DeltaColumnBuilder.scala` - Extends API DeltaColumnBuilder
- [x] `connect/DeltaMergeBuilder.scala` - Extends API merge builders (all 4 classes)
- [x] `connect/DeltaOptimizeBuilder.scala` - Extends API DeltaOptimizeBuilder
- [x] `connect/package.scala` - Registers connect as default implementation

### 3.2 Connect Implementation Notes
- Connect `convertToDelta` throws `UnsupportedOperationException` (not supported in remote mode)
- Connect clone methods updated to use `Long` for version (converted with `.toInt` internally)
- Connect adds `java.util.HashMap` overloads for clone methods
- Connect uses protobuf (io.delta.connect.proto) for remote execution

### 3.3 Old Connect Files - DELETED
- [x] `io/delta/connect/tables/DeltaTable.scala`
- [x] `io/delta/connect/tables/DeltaTableBuilder.scala`
- [x] `io/delta/connect/tables/DeltaColumnBuilder.scala`
- [x] `io/delta/connect/tables/DeltaMergeBuilder.scala`
- [x] `io/delta/connect/tables/DeltaOptimizeBuilder.scala`
- [x] `io/delta/connect/tables/execution/DeltaTableBuilderOptions.scala`
- [x] Removed empty directories: `io/delta/connect/tables/execution/`, `io/delta/connect/tables/`, `io/delta/connect/`

### 3.4 Old Classic Files at Root - DELETED
- [x] `spark/src/main/scala/io/delta/tables/DeltaTable.scala`
- [x] `spark/src/main/scala/io/delta/tables/DeltaTableBuilder.scala`
- [x] `spark/src/main/scala/io/delta/tables/DeltaColumnBuilder.scala`
- [x] `spark/src/main/scala/io/delta/tables/DeltaMergeBuilder.scala`
- [x] `spark/src/main/scala/io/delta/tables/DeltaOptimizeBuilder.scala`
- [x] `spark/src/main/scala/io/delta/tables/execution/DeltaConvert.scala`
- [x] `spark/src/main/scala/io/delta/tables/execution/DeltaTableBuilderOptions.scala`
- [x] `spark/src/main/scala/io/delta/tables/execution/DeltaTableOperations.scala`
- [x] `spark/src/main/scala/io/delta/tables/execution/VacuumTableCommand.scala`
- [x] Removed empty directory: `spark/src/main/scala/io/delta/tables/execution/`

## Phase 4: Update Tests
- [ ] Update Connect test packages
- [ ] Update imports

## Phase 5: Build System
- [ ] Update build.sbt

---

## Implementation Log

### Phase 1 Completed
Created the following files in `spark-api/src/main/scala/io/delta/tables/`:

1. **DeltaTable.scala** - Abstract class defining the public API for DeltaTable instances
2. **DeltaTableBuilder.scala** - Abstract builder + `DeltaTableBuilderOptions` sealed trait
3. **DeltaColumnBuilder.scala** - Abstract builder for column specification
4. **DeltaMergeBuilder.scala** - Abstract merge operation builders (4 classes)
5. **DeltaOptimizeBuilder.scala** - Abstract optimize builder
6. **package.scala** - Companion infrastructure with `DeltaTableCompanion`

### Phase 2 Completed
Created classic implementation in `spark/src/main/scala/io/delta/tables/classic/`:

1. **DeltaTable.scala** - Class extends `io.delta.tables.DeltaTable`, companion extends `DeltaTableCompanion`
2. **DeltaTableBuilder.scala** - Extends `io.delta.tables.DeltaTableBuilder`
3. **DeltaColumnBuilder.scala** - Extends `io.delta.tables.DeltaColumnBuilder`
4. **DeltaMergeBuilder.scala** - All 4 builder classes extend their API counterparts
5. **DeltaOptimizeBuilder.scala** - Extends `io.delta.tables.DeltaOptimizeBuilder`
6. **package.scala** - Calls `setDefaultImplementation(DeltaTable)` to register classic as default
7. **execution/** - Updated package declarations for all execution files

Key patterns used:
- Classes extend API abstract classes using fully qualified names to avoid naming conflicts
- All method overrides use `/** @inheritdoc */` for documentation inheritance
- Companion object extends `DeltaTableCompanion` and implements all factory methods

### Phase 3 Completed
Created connect implementation in `spark-connect/client/src/main/scala-spark-master/io/delta/tables/connect/`:

1. **DeltaTable.scala** - Class extends `io.delta.tables.DeltaTable`, companion extends `DeltaTableCompanion`
2. **DeltaTableBuilder.scala** - Extends `io.delta.tables.DeltaTableBuilder`
3. **DeltaColumnBuilder.scala** - Extends `io.delta.tables.DeltaColumnBuilder`
4. **DeltaMergeBuilder.scala** - All 4 builder classes extend their API counterparts
5. **DeltaOptimizeBuilder.scala** - Extends `io.delta.tables.DeltaOptimizeBuilder`
6. **package.scala** - Calls `setDefaultImplementation(DeltaTable)` to register connect as default

Key patterns used:
- Classes extend API abstract classes using fully qualified names
- All method overrides use `/** @inheritdoc */` for documentation inheritance
- Uses protobuf (io.delta.connect.proto) for remote command execution
- `convertToDelta` throws `UnsupportedOperationException` (not supported remotely)

---

## Files Created

### spark-api Module
```
spark-api/src/main/scala/io/delta/tables/
├── DeltaTable.scala           # Abstract API class
├── DeltaTableBuilder.scala    # Abstract builder + DeltaTableBuilderOptions
├── DeltaColumnBuilder.scala   # Abstract column builder
├── DeltaMergeBuilder.scala    # Abstract merge builders (4 classes)
├── DeltaOptimizeBuilder.scala # Abstract optimize builder
└── package.scala              # DeltaTableCompanion + DeltaTable object
```

### Classic Implementation (spark Module)
```
spark/src/main/scala/io/delta/tables/classic/
├── DeltaTable.scala           # Classic implementation + companion
├── DeltaTableBuilder.scala    # Classic builder
├── DeltaColumnBuilder.scala   # Classic column builder
├── DeltaMergeBuilder.scala    # Classic merge builders (4 classes)
├── DeltaOptimizeBuilder.scala # Classic optimize builder
├── package.scala              # Registers classic as default
└── execution/
    ├── DeltaConvert.scala     # Convert operations
    ├── DeltaTableOperations.scala # Table operations trait
    └── VacuumTableCommand.scala   # Vacuum command
```

### Connect Implementation (spark-connect Module)
```
spark-connect/client/src/main/scala-spark-master/io/delta/tables/connect/
├── DeltaTable.scala           # Connect implementation + companion
├── DeltaTableBuilder.scala    # Connect builder
├── DeltaColumnBuilder.scala   # Connect column builder
├── DeltaMergeBuilder.scala    # Connect merge builders (4 classes)
├── DeltaOptimizeBuilder.scala # Connect optimize builder
└── package.scala              # Registers connect as default
```
