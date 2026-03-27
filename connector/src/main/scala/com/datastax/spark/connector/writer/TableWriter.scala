/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.spark.connector.writer

import java.io.{Closeable, IOException}
import java.util.function.Supplier

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{DefaultBatchType, PreparedStatement, SimpleStatement}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, CqlTokenRange}
import com.datastax.spark.connector.types.{ListType, MapType}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util._
import com.datastax.spark.connector.writer.AsyncExecutor.Handler
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.metrics.OutputMetricsUpdater

import scala.collection._

/** Writes RDD data into given Cassandra table.
  * Individual column values are extracted from RDD objects using given [[RowWriter]]
  * Then, data are inserted into Cassandra with batches of CQL INSERT statements.
  * Each RDD partition is processed by a single thread. */
class TableWriter[T] private (
    connector: CassandraConnector,
    tableDef: TableDef,
    columnSelector: IndexedSeq[ColumnRef],
    rowWriter: RowWriter[T],
    writeConf: WriteConf,
    partitions: Array[Partition],
    tokenRangeAcc: Option[TokenRangeAccumulator],
    private val isDelete: Boolean = false,
    private var cachedDeleteQueryTemplate: Option[String] = None,
    syntheticColumnNames: Set[String] = Set.empty) extends Serializable with Logging {

  require(!tableDef.isView,
    s"${tableDef.name} is a Materialized View and Views are not writable")

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames diff writeConf.optionPlaceholders
  val columns = columnNames.map(tableDef.columnByName)

  private lazy val tableDefWithoutPlaceholders: TableDef =
    if (syntheticColumnNames.isEmpty) tableDef
    else tableDef.copy(
      regularColumns = tableDef.regularColumns.filterNot(c => syntheticColumnNames.contains(c.columnName))
    )

  private[connector] lazy val queryTemplateUsingInsert: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")

    val ifNotExistsSpec = if (writeConf.ifNotExists) "IF NOT EXISTS " else ""

    val options = List(ttlSpec, timestampSpec).flatten
    val optionsSpec = if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $ifNotExistsSpec$optionsSpec".trim
  }

  // A `def` because `deleteColumns` is a parameter, not a property of the writer.
  // Called once by `TableWriter.forDelete` which caches the result in `cachedDeleteQueryTemplate`.
  // Precondition: `columns` (the key columns for the WHERE clause) must contain only primary key
  // columns. This is enforced by the caller (RDDFunctions.deleteFromCassandra passes PK-only
  // columns), and validated by the primary-key-only check below.
  private[writer] def deleteQueryTemplate(deleteColumns: ColumnSelector): String = {
    // CQL DELETE does not support collection-element operations (append, prepend, remove).
    // Reject any CollectionColumnName before building the query.
    deleteColumns match {
      case sc: SomeColumns =>
        val collectionRefs = sc.columns.collect { case c: CollectionColumnName => c.columnName }
        if (collectionRefs.nonEmpty) {
          throw new IllegalArgumentException(
            s"CQL DELETE does not support collection behaviors. Collection columns found in delete columns: ${collectionRefs.mkString(", ")}")
        }
      case _ => // AllColumns, PrimaryKeyColumns, etc. never produce CollectionColumnName
    }

    // Validate column existence before selectFrom to give a DELETE-specific error message.
    // selectFrom throws a generic NoSuchElementException that doesn't mention DELETE.
    deleteColumns match {
      case sc: SomeColumns =>
        val tableColumnNames = tableDefWithoutPlaceholders.columns.map(_.columnName).toSet
        val missing = sc.columns.map(_.columnName).filterNot(tableColumnNames.contains)
        if (missing.nonEmpty)
          throw new IllegalArgumentException(
            s"Column(s) not found for DELETE in table ${tableDef.tableName}: ${missing.mkString(", ")}")
      case _ =>
    }

    val primaryKeyNames = tableDef.primaryKey.map(_.columnName).toSet
    // selectFrom uses tableDefWithoutPlaceholders so that synthetic option-placeholder columns are excluded
    val allDeleteColumnNames: Seq[String] = deleteColumns.selectFrom(tableDefWithoutPlaceholders).map(_.columnName)
    // When AllColumns is used, auto-filter primary key columns since they can't be deleted
    val deleteColumnNames = if (deleteColumns == AllColumns) {
      val filtered = allDeleteColumnNames.filterNot(primaryKeyNames.contains)
      if (filtered.isEmpty)
        logInfo(s"Table ${tableDef.tableName} has no non-PK columns; AllColumns delete will perform a full-row delete")
      filtered
    } else {
      allDeleteColumnNames
    }
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    if (regularColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"Only primary key columns can be specified as key columns for DELETE. Regular columns found: ${regularColumns.mkString(", ")}")
    }
    // Empty deleteColumnNames = full row delete; PK column check only applies to column-level deletes
    if (deleteColumnNames.nonEmpty) {
      val pkInDelete = deleteColumnNames.filter(primaryKeyNames.contains)
      if (pkInDelete.nonEmpty) {
        throw new IllegalArgumentException(
          s"Primary key columns cannot be specified as delete columns. Primary key columns found: ${pkInDelete.mkString(", ")}")
      }
    }
    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val deleteColumnsClause = deleteColumnNames.map(quote).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    val usingTimestampClause = timestampSpec.map(ts => s" USING $ts").getOrElse("")
    val deleteSpec = if (deleteColumnsClause.nonEmpty) s"$deleteColumnsClause " else ""

    s"DELETE ${deleteSpec}FROM ${quote(keyspaceName)}.${quote(tableName)}$usingTimestampClause WHERE $whereClause"
  }

  private[writer] lazy val queryTemplateUsingUpdate: String = {
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    val (counterColumns, nonCounterColumns) = regularColumns.partition(_.isCounterColumn)

    val nameToBehavior = (columnSelector collect {
        case cn:CollectionColumnName => cn.columnName -> cn.collectionBehavior
      }).toMap

    val setNonCounterColumnsClause = for {
      colDef <- nonCounterColumns
      name = colDef.columnName
      collectionBehavior = nameToBehavior.get(name)
      quotedName = quote(name)
    } yield collectionBehavior match {
        case Some(CollectionAppend)           => s"$quotedName = $quotedName + :$quotedName"
        case Some(CollectionPrepend)          => s"$quotedName = :$quotedName + $quotedName"
        case Some(CollectionRemove)           => s"$quotedName = $quotedName - :$quotedName"
        case Some(CollectionOverwrite) | None => s"$quotedName = :$quotedName"
      }

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val setCounterColumnsClause = quotedColumnNames(counterColumns).map(c => s"$c = $c + :$c")
    val setClause = (setNonCounterColumnsClause ++ setCounterColumnsClause).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    // Counter updates don't support USING TIMESTAMP or USING TTL.
    // For all other UPDATEs, assign per-statement timestamps to ensure that multiple
    // mutations to the same list within a batch get distinct timestamps. Without this,
    // Scylla drops all but one list mutation when they share the same timestamp.
    // See: https://github.com/scylladb/spark-scylladb-connector/issues/26
    // Note: TTL is now also applied to UPDATE statements when writeConf.ttl is set.
    // Previously TTL was only included in INSERT statements.
    val usingClause = if (isCounterUpdate) {
      ""
    } else {
      val autoTs = Some(s"TIMESTAMP :${TableWriter.AutoTimestampParam}")
      val parts = List(ttlSpec, timestampSpec.orElse(autoTs)).flatten
      if (parts.nonEmpty) s" USING ${parts.mkString(" AND ")}" else ""
    }

    s"UPDATE ${quote(keyspaceName)}.${quote(tableName)}${usingClause} SET $setClause WHERE $whereClause"
  }

  private lazy val timestampSpec: Option[String] = {
    writeConf.timestamp match {
      case TimestampOption(PerRowWriteOptionValue(placeholder)) => Some(s"TIMESTAMP :$placeholder")
      case TimestampOption(StaticWriteOptionValue(value)) => Some(s"TIMESTAMP $value")
      case _ => None
    }
  }

  private lazy val ttlSpec: Option[String] = {
    writeConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder)) => Some(s"TTL :$placeholder")
      case TTLOption(StaticWriteOptionValue(value)) => Some(s"TTL $value")
      case _ => None
    }
  }

  private val isCounterUpdate =
    tableDef.columns.exists(_.isCounterColumn)

  private val containsCollectionBehaviors =
    columnSelector.exists(_.isInstanceOf[CollectionColumnName])

  private[connector] lazy val isIdempotent: Boolean = {
    // DELETEs are inherently idempotent
    if (isDelete) {
      true
    //All counter operations are not Idempotent
    } else if (columns.exists(_.isCounterColumn)) {
       false
    } else {
      columnSelector.forall {
        //Any appends or prepends to a list are non-idempotent
        case cn: CollectionColumnName =>
          val name = cn.columnName
          val behavior = cn.collectionBehavior
          val isNotList = !tableDef.columnByName(name).columnType.isInstanceOf[ListType[_]]
          behavior match {
            case CollectionPrepend => isNotList
            case CollectionAppend => isNotList
            case _ => true
          }
       //All other operations on regular columns are idempotent
       case regularColumn: ColumnRef => true
      }
    }
  }

  private def prepareStatement(queryTemplate:String, session: CqlSession): PreparedStatement = {
    try {
      val stmt = SimpleStatement.newInstance(queryTemplate)
        .setIdempotent(isIdempotent)
        .setConsistencyLevel(writeConf.consistencyLevel)
      session.prepare(stmt)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Failed to prepare statement $queryTemplate: " + t.getMessage, t)
    }
  }

  def batchRoutingKey(session: CqlSession): RichBoundStatementWrapper => Any = {
    writeConf.batchGroupingKey match {
      case BatchGroupingKey.None => (_: RichBoundStatementWrapper) => 0

      case BatchGroupingKey.ReplicaSet =>
        val tokenMap = session.getMetadata.getTokenMap.orElseThrow(
          new Supplier[IllegalArgumentException] {
            override def get(): IllegalArgumentException = new IllegalArgumentException("TokenMap Metadata Missing")
          })
        (bs: RichBoundStatementWrapper) => tokenMap.getReplicas(keyspaceName, QueryUtils.getRoutingKeyOrError(bs.stmt))

      case BatchGroupingKey.Partition =>
        (bs: RichBoundStatementWrapper) => QueryUtils.getRoutingKeyOrError(bs.stmt)
    }
  }

  /**
    * Main entry point
    * if counter or collection column need to be updated Cql UPDATE command will be used
    * INSERT otherwise
    */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    require(!isDelete, "write() must not be called on a delete-configured TableWriter; use delete() instead")
    val asyncStatementWriter = getAsyncWriter()
    writeInternal(asyncStatementWriter, taskContext, data)
  }

  /**
    * Cql DELETE statement. The writer must be created via [[TableWriter.forDelete]] which
    * eagerly validates and caches the delete query template.
    */
  def delete(taskContext: TaskContext, data: Iterator[T]): Unit = {
    val template = cachedDeleteQueryTemplate.getOrElse(
      throw new IllegalStateException(
        "delete() requires a cached delete query template; create the writer with TableWriter.forDelete()"))
    writeInternal(getAsyncWriterInternal(template), taskContext, data)
  }

  def extractTokenRange(partitionId: Int): Iterable[CqlTokenRange[_, _]] =
    partitions.lift(partitionId) match {
      case Some(CassandraPartition(_, _, ranges, _)) => ranges
      case _ => List()
    }

  def getAsyncWriter(): AsyncStatementWriter[T] = {
    require(!isDelete, "getAsyncWriter() must not be called on a delete-configured TableWriter; use delete() instead")
    if (isCounterUpdate || containsCollectionBehaviors) {
      getAsyncWriterInternal(queryTemplateUsingUpdate)
    }
    else {
      getAsyncWriterInternal(queryTemplateUsingInsert)
    }
  }

  private def getAsyncWriterInternal(queryTemplate: String): AsyncStatementWriter[T] = {
    connector.withSessionDo { session =>
      val protocolVersion = session.getContext.getProtocolVersion
      val stmt = prepareStatement(queryTemplate, session)
      val batchType = if (!isDelete && isCounterUpdate) DefaultBatchType.COUNTER else DefaultBatchType.UNLOGGED

      val boundStmtBuilder = new BoundStatementBuilder(
        rowWriter,
        stmt,
        protocolVersion = protocolVersion,
        ignoreNulls = writeConf.ignoreNulls)

      val batchStmtBuilder = new BatchStatementBuilder(batchType, writeConf.consistencyLevel)
      val batchKeyGenerator = batchRoutingKey(session)
      val batchBuilder = new GroupingBatchBuilderBase(boundStmtBuilder, batchStmtBuilder, batchKeyGenerator,
        writeConf.batchSize, writeConf.batchGroupingBufferSize)

      val maybeRateLimit: RichStatement => Unit = writeConf.throughputMiBPS match {
        case Some(throughput) =>
          val rateLimiter = new RateLimiter(
            (throughput * 1024 * 1024).toLong,
            1024 * 1024)
          (stmt: RichStatement) => rateLimiter.maybeSleep(stmt.bytesCount)
        case None =>
          (stmt: RichStatement) => ()
      }

      AsyncStatementWriter(connector, writeConf, tableDef, stmt, batchBuilder, maybeRateLimit)
    }
  }

  private def writeInternal(asyncStatementWriter: AsyncStatementWriter[T], taskContext: TaskContext, data: Iterator[T]): Unit = {
    val updater = OutputMetricsUpdater(taskContext, writeConf)
    val tokenRanges = extractTokenRange(taskContext.partitionId())
    logInfo(s"Writing ranges: ${tokenRanges}")

    val metricMonitoringWriter = asyncStatementWriter.copy(
        successHandler = Some(updater.batchFinished(success = true, _, _, _)),
        failureHandler = Some(updater.batchFinished(success = false, _, _, _)))

    val rowIterator = new CountingIterator(data)

    logDebug(s"Writing data partition to $keyspaceName.$tableName in batches of ${writeConf.batchSize}.")

    for (stmtToWrite <- rowIterator) {
      metricMonitoringWriter.write(stmtToWrite)
    }

    metricMonitoringWriter.close()

    val duration = updater.finish() / 1000000000d
    logInfo(f"Wrote ${rowIterator.count} rows to $keyspaceName.$tableName in $duration%.3f s.")

    tokenRangeAcc.foreach(_.add(tokenRanges.toSet))
    logInfo("Added token ranges to accumulator")
  }
}

case class AsyncStatementWriter[T](
  connector: CassandraConnector,
  writeConf: WriteConf,
  tableDef: TableDef,
  preparedStatement: PreparedStatement,
  groupingBatchBuilderBase: GroupingBatchBuilderBase[T],
  maybeRateLimit: RichStatement => Unit,
  successHandler: Option[Handler[RichStatement]] = None,
  failureHandler: Option[Handler[RichStatement]] = None)
  extends Closeable
    with Logging {

  //Don't grab a connection or queryExecutor unless we are using this statement writer
  private lazy val session: CqlSession = connector.openSession()
  private val keyspaceName: String = tableDef.keyspaceName
  private val tableName: String = tableDef.tableName

  private lazy val queryExecutor = new QueryExecutor(
    session, writeConf.parallelismLevel, successHandler, failureHandler,
    maxRetries = connector.conf.queryRetryMaxRetries)

  def write(record: T): Unit= {
    groupingBatchBuilderBase.batchRecord(record).foreach{ stmt =>
      maybeRateLimit(stmt)
      queryExecutor.executeAsync(stmt.executeAs(writeConf.executeAs))
    }
  }

  override def close(): Unit = {
    for (statement <- groupingBatchBuilderBase.finish()) {
      maybeRateLimit(statement)
      queryExecutor.executeAsync(statement.executeAs(writeConf.executeAs))
    }

    queryExecutor.waitForCurrentlyExecutingTasks()
    queryExecutor.getLatestException().map {
      case exception =>
        throw new IOException(
          s"""Failed to write statements to $keyspaceName.$tableName. The
             |latest exception was
             |  ${exception.getMessage}
             |
             |Please check the executor logs for more exceptions and information
             """.stripMargin)
    }
    session.close()
  }
}

object TableWriter {

  /** Name of the auto-generated timestamp bind parameter added to UPDATE statements. */
  private[writer] val AutoTimestampParam = "connector_autots"

  private def checkOptionPlaceholderCollisions(table: TableDef, writeConf: WriteConf): Unit = {
    val allColumnNames = (table.partitionKey ++ table.clusteringColumns ++ table.regularColumns)
      .map(_.columnName).toSet
    if (allColumnNames.contains(AutoTimestampParam))
      throw new IllegalArgumentException(
        s"Table column '$AutoTimestampParam' conflicts with internal auto-timestamp parameter. " +
        "Rename the column or use an explicit timestamp via WriteConf.")
    if (writeConf.optionPlaceholders.contains(AutoTimestampParam))
      throw new IllegalArgumentException(
        s"Per-row placeholder name '$AutoTimestampParam' conflicts with internal auto-timestamp parameter.")
  }

  private def checkMissingColumns(table: TableDef, columnNames: Seq[String]): Unit = {
    val allColumnNames = table.columns.map(_.columnName)
    val missingColumns = columnNames.toSet -- allColumnNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Column(s) not found: ${missingColumns.mkString(", ")}")
  }

  private def checkMissingPrimaryKeyColumns(table: TableDef, columnNames: Seq[String]): Unit = {
    val primaryKeyColumnNames = table.primaryKey.map(_.columnName)
    val missingPrimaryKeyColumns = primaryKeyColumnNames.toSet -- columnNames
    if (missingPrimaryKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some primary key columns are missing in RDD or have not been selected: ${missingPrimaryKeyColumns.mkString(", ")}")
  }

  private def checkMissingPartitionKeyColumns(table: TableDef, columnNames: Seq[String]): Unit = {
    val partitionKeyColumnNames = table.partitionKey.map(_.columnName)
    val missingPartitionKeyColumns = partitionKeyColumnNames.toSet -- columnNames
    if (missingPartitionKeyColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Some partition key columns are missing in RDD or have not been selected: ${missingPartitionKeyColumns.mkString(", ")}")
  }

  private def onlyPartitionKeyAndStatic(table: TableDef, columnNames: Seq[String]): Boolean = {
    val nonPartitionKeyColumnNames = columnNames.toSet -- table.partitionKey.map(_.columnName)
    val nonPartitionKeyColumnRefs = table
      .allColumns
      .filter(columnDef => nonPartitionKeyColumnNames.contains(columnDef.columnName))
    nonPartitionKeyColumnRefs.forall( columnDef => columnDef.columnRole == StaticColumn)
  }

  /**
   * Check whether a collection behavior is being applied to a non collection column
   * Check whether prepend is used on any Sets or Maps
   * Check whether remove is used on Maps
   */
  private def checkCollectionBehaviors(table: TableDef, columnRefs: IndexedSeq[ColumnRef]): Unit = {
    val tableCollectionColumns = table.columns.filter(cd => cd.isCollection)
    val tableCollectionColumnNames = tableCollectionColumns.map(_.columnName)
    val tableListColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: ListType[_]) => name }

    val tableMapColumnNames = tableCollectionColumns
      .map(c => (c.columnName, c.columnType))
      .collect { case (name, x: MapType[_, _]) => name }

    val refsWithCollectionBehavior = columnRefs collect {
      case columnName: CollectionColumnName => columnName
    }

    val collectionBehaviorColumnNames = refsWithCollectionBehavior.map(_.columnName)

    //Check for non-collection columns with a collection Behavior
    val collectionBehaviorNormalColumn =
      collectionBehaviorColumnNames.toSet -- tableCollectionColumnNames.toSet

    if (collectionBehaviorNormalColumn.nonEmpty)
      throw new IllegalArgumentException(
        s"""Collection behaviors (add/remove/append/prepend) are only allowed on collection columns.
           |Normal Columns with illegal behavior: ${collectionBehaviorNormalColumn.mkString}"""
          .stripMargin
      )

    //Check that prepend is used only on lists
    val prependBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionPrepend)
      .map(_.columnName)
    val prependOnNonList = prependBehaviorColumnNames.toSet -- tableListColumnNames.toSet

    if (prependOnNonList.nonEmpty)
      throw new IllegalArgumentException(
        s"""The prepend collection behavior only applies to Lists. Prepend used on:
           |${prependOnNonList.mkString}""".stripMargin
      )

    //Check that remove is not used on Maps

    val removeBehaviorColumnNames = refsWithCollectionBehavior
      .filter(_.collectionBehavior == CollectionRemove)
      .map(_.columnName)

    val removeOnMap = removeBehaviorColumnNames.toSet & tableMapColumnNames.toSet

    if (removeOnMap.nonEmpty)
      throw new IllegalArgumentException(
        s"The remove operation is currently not supported for Maps. Remove used on: ${removeOnMap
          .mkString}"
      )
  }

  private def checkColumns(table: TableDef, columnRefs: IndexedSeq[ColumnRef], partitionKeyOnly: Boolean) = {
    val columnNames = columnRefs.map(_.columnName)
    checkMissingColumns(table, columnNames)
    if (partitionKeyOnly) {
      // For Deletes we only need a partition Key for a valid delete statement
      checkMissingPartitionKeyColumns(table, columnNames)
    }
    else if (onlyPartitionKeyAndStatic(table, columnNames)) {
      // Cassandra only requires a Partition Key Column on insert if all other columns are Static
      checkMissingPartitionKeyColumns(table, columnNames)
    }
    else {
      // For all other normal Cassandra writes we require the full primary key to be present
      checkMissingPrimaryKeyColumns(table, columnNames)
    }
    checkCollectionBehaviors(table, columnRefs)
  }

  /** Creates a [[TableWriter]] configured for DELETE operations.
    * Determines whether this is a column-level or full-row delete, adjusts the
    * [[WriteConf]] (stripping TTL and other INSERT-only options), and constructs
    * the writer with the appropriate key-column validation mode.
    * Eagerly validates the delete query template on the driver to fail fast
    * rather than wasting cluster resources on executor-side validation. */
  private[connector] def forDelete[T : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      deleteColumns: ColumnSelector,
      keyColumns: ColumnSelector,
      writeConf: WriteConf): TableWriter[T] = {
    keyColumns match {
      case AllColumns => throw new IllegalArgumentException(
        "AllColumns cannot be used as keyColumns for DELETE; use PrimaryKeyColumns or SomeColumns(...)")
      case _ =>
    }
    deleteColumns match {
      case PrimaryKeyColumns | PartitionKeyColumns =>
        throw new IllegalArgumentException(
          s"$deleteColumns cannot be used as deleteColumns for DELETE; " +
          "use SomeColumns(...) to delete specific columns or SomeColumns() for full-row delete")
      case _ =>
    }
    // column delete requires full primary key, partition key is enough otherwise
    val columnDelete = deleteColumns match {
      case c: SomeColumns => c.columns.nonEmpty
      case AllColumns     => true
      case _              => false
    }
    val deleteConf = writeConf.forDelete
    val writer = TableWriter[T](connector, keyspaceName, tableName, keyColumns, deleteConf,
      partitionKeyOnly = !columnDelete, isDelete = true)
    // Eagerly compute and cache the delete query template on the driver side.
    // This triggers all validation (column existence, PK checks, collection checks)
    // before the Spark job is submitted, avoiding wasted cluster resources.
    writer.cachedDeleteQueryTemplate = Some(writer.deleteQueryTemplate(deleteColumns))
    writer
  }

  /** Columns that cannot actually be written to because they represent virtual endpoints
    */
  private val InternalColumns = Set("solr_query")

  def apply[T : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      columnNames: ColumnSelector,
      writeConf: WriteConf,
      partitionKeyOnly: Boolean = false,
      partitions: Array[Partition] = Array(),
      tokenRangeAcc: Option[TokenRangeAccumulator] = None,
      isDelete: Boolean = false): TableWriter[T] = {

    val tableDef = tableFromCassandra(connector, keyspaceName, tableName)
    TableWriter(connector, tableDef, columnNames, writeConf, partitionKeyOnly, partitions, tokenRangeAcc, isDelete)
  }

  def apply[T : RowWriterFactory](
       connector: CassandraConnector,
       tableDef: TableDef,
       columnNames: ColumnSelector,
       writeConf: WriteConf,
       partitionKeyOnly: Boolean,
       partitions: Array[Partition],
       tokenRangeAcc: Option[TokenRangeAccumulator],
       isDelete: Boolean): TableWriter[T] = {

    val optionColumns = writeConf.optionsAsColumns
    val existingColumnNames = tableDef.columns.map(_.columnName).toSet
    val newOptionColumns = optionColumns.filterNot(c => existingColumnNames.contains(c.columnName))
    val tablDefWithMeta = tableDef.copy(regularColumns = tableDef.regularColumns ++ newOptionColumns)

    checkOptionPlaceholderCollisions(tableDef, writeConf)

    val baseSelectedColumns = columnNames
      .selectFrom(tablDefWithMeta)
      .filter(col => !InternalColumns.contains(col.columnName))
    // Ensure per-row option placeholder columns (e.g. per-row timestamp/TTL) are included
    // even when columnNames is PrimaryKeyColumns or AllColumns, which would not select them.
    val selectedColumnNames = baseSelectedColumns.map(_.columnName).toSet
    val missingOptionColumns = optionColumns.filterNot(c => selectedColumnNames.contains(c.columnName))
    val selectedColumns = baseSelectedColumns ++ missingOptionColumns.map(c => ColumnName(c.columnName))

    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(tablDefWithMeta, selectedColumns)

    checkColumns(tablDefWithMeta, selectedColumns, partitionKeyOnly)
    new TableWriter[T](connector, tablDefWithMeta, selectedColumns, rowWriter, writeConf, partitions, tokenRangeAcc, isDelete,
      syntheticColumnNames = newOptionColumns.map(_.columnName).toSet)
  }
}
