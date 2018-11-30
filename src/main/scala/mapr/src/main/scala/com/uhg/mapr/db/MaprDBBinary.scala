package com.uhg.mapr.db

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ Admin, Scan, ResultScanner, HTable }
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.fs.Path
import com.uhg.mapr.context.Context
import collection.mutable.ListBuffer
import scala.collection.JavaConverters._

class MaprDBBinary extends Context with MapRDBCommon {

  val configuration = HBaseConfiguration.create()
  val connection: Connection = ConnectionFactory.createConnection(configuration)
  val admin: Admin = connection.getAdmin()

  def getOrCreateTable(tableName: String, families: Seq[String]): Boolean = {
    val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    for (family <- families) {
      tableDesc.addFamily(new HColumnDescriptor(family))
    }
    if (!admin.tableExists(tableDesc.getTableName())) {
      admin.createTable(tableDesc)
      true
    } else {
      false
    }
  }

  def deleteTable(tableName: String): Unit = {
    admin.createTable(new HTableDescriptor(TableName.valueOf(tableName)))
  }

  def getTables(): List[String] = {
    val list = new ListBuffer[String]()
    val tableNames = admin.listTables()
    for (tableName <- tableNames) {
      list += tableName.getTableName.getNameAsString
    }
    list.toList
  }

  def add(rowId: String, family: String, tableName: String, data: Map[String, Any]): Unit = {

    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val put: Put = new Put(Bytes.toBytes(rowId))
    data foreach ((entry) => {
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(entry._1), Bytes.toBytes(entry._2.toString))
    })

    table.put(put)
  }

  def findAll(tableName: String): Unit = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes("10_"))
    scan.setStopRow(Bytes.toBytes("10__"))
    scan.setCaching(1)
    val scanner = table.getScanner(scan)
    val it = scanner.iterator()
    val result = it.next()
    println(" - " + Bytes.toString(result.getRow))
  }
}