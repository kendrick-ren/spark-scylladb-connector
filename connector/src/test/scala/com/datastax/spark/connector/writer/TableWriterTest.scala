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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.net.InetSocketAddress

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{CounterType, IntType, TextType}
import org.junit.{Assert, Test}

class TableWriterTest {

  private def createConnector(): CassandraConnector = {
    val contactInfo = IpBasedContactInfo(Set(new InetSocketAddress("127.0.0.1", 9042)))
    val connectorConf = CassandraConnectorConf(contactInfo)
    new CassandraConnector(connectorConf)
  }

  private def createTableDef(regularColumns: Seq[ColumnDef] = Seq.empty): TableDef = {
    val pk = ColumnDef("pk", PartitionKeyColumn, IntType)
    val ck = ColumnDef("ck", ClusteringColumn(0), IntType)
    TableDef("test_ks", "test_table", Seq(pk), Seq(ck), regularColumns)
  }

  @Test
  def deleteClosureShouldBeSerializable(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val deleteFunc = writer.delete _

    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(deleteFunc) // must not throw NotSerializableException
    oos.close()
  }

  @Test
  def deleteClosureWithPerRowTimestampShouldBeSerializable(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()
    val writeConf = WriteConf(timestamp = TimestampOption.perRow("_ts"))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, writeConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val deleteFunc = writer.delete _

    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(deleteFunc) // must not throw NotSerializableException
    oos.close()
  }

  @Test
  def deleteWithPerRowTtlShouldSucceedWhenKeyColumnsOmitPlaceholder(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()
    val writeConf = WriteConf(ttl = TTLOption.perRow("ignored_ttl"))
    val deleteConf = writeConf.forDelete

    // forDelete keeps per-row TTL for positional alignment; the DELETE template omits it
    Assert.assertTrue(
      "forDelete should keep per-row TTL placeholder",
      deleteConf.optionPlaceholders.contains("ignored_ttl"))

    // Building a TableWriter with PrimaryKeyColumns and the delete conf should succeed
    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, deleteConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = true)

    val deleteFunc = writer.delete _

    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(deleteFunc)
    oos.close()
  }

  @Test
  def deleteQueryTemplateShouldRejectRegularColumnsAsKeyColumns(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(), partitionKeyOnly = false,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    try {
      writer.deleteQueryTemplate(SomeColumns())
      Assert.fail("Expected IllegalArgumentException for regular columns as key columns")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue(e.getMessage.contains("Regular columns found"))
    }
  }

  @Test
  def deleteQueryTemplateWithAllColumnsShouldAutoFilterPrimaryKeyColumns(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(AllColumns)
    Assert.assertTrue("Should contain the regular column", query.contains("\"value\""))
    Assert.assertFalse("Should not contain pk column in delete spec", query.contains("\"pk\" FROM"))
    Assert.assertFalse("Should not contain ck column in delete spec", query.contains("\"ck\" FROM"))
  }

  @Test
  def deleteQueryTemplateWithAllColumnsOnPkOnlyTableShouldProduceFullRowDelete(): Unit = {
    val connector = createConnector()
    // Table with only primary key columns (no regular columns)
    val tableDef = createTableDef(regularColumns = Seq.empty)

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(AllColumns)
    // Should be a full-row DELETE (no column list before FROM)
    Assert.assertTrue("Should be a full-row DELETE", query.startsWith("DELETE FROM"))
    Assert.assertTrue("Should contain WHERE clause", query.contains("WHERE"))
  }

  @Test
  def deleteWithPerRowTimestampAndPrimaryKeyColumnsShouldIncludePlaceholder(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()
    val writeConf = WriteConf(timestamp = TimestampOption.perRow("_ts"))

    // PrimaryKeyColumns would not normally select the synthetic "_ts" column.
    // The fix ensures it is automatically included.
    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, writeConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(SomeColumns())
    Assert.assertTrue("Delete query should contain USING TIMESTAMP :_ts",
      query.contains("USING TIMESTAMP :_ts"))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def deleteQueryTemplateShouldRejectCollectionColumnNameInDeleteColumns(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    writer.deleteQueryTemplate(SomeColumns("value".append))
  }

  @Test
  def deleteQueryTemplateShouldRejectExplicitPrimaryKeyColumnsAsDeleteColumns(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    try {
      writer.deleteQueryTemplate(SomeColumns("pk"))
      Assert.fail("Expected IllegalArgumentException for primary key columns in deleteColumns")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue(e.getMessage.contains("Primary key columns cannot be specified as delete columns"))
    }
  }

  @Test
  def insertQueryTemplateShouldIncludeIfNotExistsWithTtlAndTimestamp(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns,
      WriteConf(ifNotExists = true, ttl = TTLOption.constant(100),
        timestamp = TimestampOption.constant(1400000000000L)),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingInsert
    Assert.assertTrue("INSERT should contain IF NOT EXISTS",
      query.contains("IF NOT EXISTS"))
    Assert.assertTrue("INSERT should contain USING TTL 100 AND TIMESTAMP 1400000000000",
      query.contains("USING TTL 100 AND TIMESTAMP 1400000000000"))
    // IF NOT EXISTS must appear before USING clause in valid CQL
    val ifNotExistsIdx = query.indexOf("IF NOT EXISTS")
    val usingIdx = query.indexOf("USING")
    Assert.assertTrue("IF NOT EXISTS should appear before USING clause",
      ifNotExistsIdx < usingIdx)
  }

  @Test
  def forDeleteShouldStripIfNotExistsIgnoreNullsAndStaticTtl(): Unit = {
    val writeConf = WriteConf(
      ifNotExists = true,
      ignoreNulls = true,
      ttl = TTLOption.constant(100),
      timestamp = TimestampOption.constant(1400000000000L))
    val deleteConf = writeConf.forDelete

    Assert.assertFalse("forDelete should strip ifNotExists", deleteConf.ifNotExists)
    Assert.assertFalse("forDelete should strip ignoreNulls", deleteConf.ignoreNulls)
    // Static TTL is stripped because CQL DELETE does not support TTL
    Assert.assertEquals("forDelete should strip static TTL", TTLOption.defaultValue, deleteConf.ttl)
    Assert.assertEquals("forDelete should preserve timestamp",
      TimestampOption.constant(1400000000000L), deleteConf.timestamp)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldThrowWhenPlaceholderCollidesWithAutoTimestampParam(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns,
      WriteConf(timestamp = TimestampOption.perRow(TableWriter.AutoTimestampParam)),
      partitionKeyOnly = true, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)
  }

  @Test
  def updateQueryShouldAlwaysIncludeAutoTimestamp(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("value", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingUpdate
    Assert.assertTrue("UPDATE should contain auto-timestamp even without collection behaviors",
      query.contains(s"USING TIMESTAMP :${TableWriter.AutoTimestampParam}"))
  }

  @Test
  def deleteQueryTemplateShouldGiveDeleteSpecificErrorForMissingColumns(): Unit = {
    val connector = createConnector()
    val tableDef = createTableDef()

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, WriteConf(), partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    try {
      writer.deleteQueryTemplate(SomeColumns("nonexistent"))
      Assert.fail("Expected IllegalArgumentException for nonexistent delete column")
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue("Error should mention DELETE",
          e.getMessage.contains("DELETE"))
        Assert.assertTrue("Error should mention the missing column",
          e.getMessage.contains("nonexistent"))
    }
  }

  @Test
  def autotsShouldNotCollideWithUserColumnNamedAutots(): Unit = {
    val connector = createConnector()
    val regular = ColumnDef("autots", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(regular))

    // Should NOT throw — the internal parameter name no longer collides with "autots"
    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingUpdate
    Assert.assertTrue("UPDATE should contain the user column 'autots'",
      query.contains("\"autots\""))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def forDeleteShouldRejectAllColumnsAsKeyColumns(): Unit = {
    val connector = createConnector()
    // AllColumns check happens before any network call, so this should throw immediately
    TableWriter.forDelete[CassandraRow](
      connector, "test_ks", "test_table",
      deleteColumns = SomeColumns(),
      keyColumns = AllColumns,
      writeConf = WriteConf())
  }

  @Test(expected = classOf[IllegalArgumentException])
  def forDeleteShouldRejectPrimaryKeyColumnsAsDeleteColumns(): Unit = {
    val connector = createConnector()
    TableWriter.forDelete[CassandraRow](
      connector, "test_ks", "test_table",
      deleteColumns = PrimaryKeyColumns,
      keyColumns = PrimaryKeyColumns,
      writeConf = WriteConf())
  }

  @Test(expected = classOf[IllegalArgumentException])
  def forDeleteShouldRejectPartitionKeyColumnsAsDeleteColumns(): Unit = {
    val connector = createConnector()
    TableWriter.forDelete[CassandraRow](
      connector, "test_ks", "test_table",
      deleteColumns = PartitionKeyColumns,
      keyColumns = PrimaryKeyColumns,
      writeConf = WriteConf())
  }

  @Test
  def deleteAllColumnsShouldIncludeRealColumnWhenPlaceholderNameMatchesRealColumn(): Unit = {
    val connector = createConnector()
    val ttlCol = ColumnDef("ttl_col", RegularColumn, IntType)
    val data = ColumnDef("data", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(ttlCol, data))
    val writeConf = WriteConf(ttl = TTLOption.perRow("ttl_col"))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, writeConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.deleteQueryTemplate(AllColumns)
    Assert.assertTrue("DELETE should include real column ttl_col even though it matches placeholder name",
      query.contains("\"ttl_col\""))
    Assert.assertTrue("DELETE should include data column",
      query.contains("\"data\""))
  }

  @Test
  def deleteSomeColumnsShouldFindRealColumnWhenPlaceholderNameMatchesRealColumn(): Unit = {
    val connector = createConnector()
    val ttlCol = ColumnDef("ttl_col", RegularColumn, IntType)
    val data = ColumnDef("data", RegularColumn, TextType)
    val tableDef = createTableDef(regularColumns = Seq(ttlCol, data))
    val writeConf = WriteConf(ttl = TTLOption.perRow("ttl_col"))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, PrimaryKeyColumns, writeConf, partitionKeyOnly = true,
      partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    // Should not throw — ttl_col is a real column even though it matches the placeholder name
    val query = writer.deleteQueryTemplate(SomeColumns("ttl_col"))
    Assert.assertTrue("DELETE should reference ttl_col", query.contains("\"ttl_col\""))
  }

  @Test
  def counterTableUpdateQueryShouldNotContainUsingTTL(): Unit = {
    val connector = createConnector()
    val counter = ColumnDef("c", RegularColumn, CounterType)
    val pk = ColumnDef("pk", PartitionKeyColumn, IntType)
    val tableDef = TableDef("test_ks", "test_counters", Seq(pk), Seq.empty, Seq(counter))

    val writer = TableWriter[CassandraRow](
      connector, tableDef, AllColumns, WriteConf(ttl = TTLOption.constant(100)),
      partitionKeyOnly = false, partitions = Array.empty, tokenRangeAcc = None, isDelete = false)

    val query = writer.queryTemplateUsingUpdate
    Assert.assertFalse("Counter UPDATE should not contain USING TTL", query.contains("USING TTL"))
    Assert.assertFalse("Counter UPDATE should not contain USING TIMESTAMP", query.contains("USING TIMESTAMP"))
  }
}
