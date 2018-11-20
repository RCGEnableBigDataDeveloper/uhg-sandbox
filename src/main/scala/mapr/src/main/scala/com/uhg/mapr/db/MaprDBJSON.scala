package com.uhg.mapr.db

import com.mapr.db.{ Admin, Table, MapRDB }
import org.ojai.store.QueryCondition
import org.ojai.store.QueryCondition.Op
import org.ojai.{ Document, DocumentStream }
import org.ojai.types.ODate
import org.apache.hadoop.fs.Path
import java.util.UUID
import com.uhg.mapr.context.Context
import scala.collection.JavaConversions._

class MaprDBJSON extends Context with MapRDBCommon {

  val admin: Admin = MapRDB.newAdmin()

  def getOrCreateTable(tableName: String): Table = {
    if (!MapRDB.tableExists(tableName)) {
      admin.createTable(new Path(tableName))
    } else {
      MapRDB.getTable(tableName)
    }
  }

  def deletTable(tableName: String): Boolean = {
    if (MapRDB.tableExists(tableName))
      admin.deleteTable(new Path(tableName))
    false
  }

  def getTables(path: String): java.util.List[Path] = { admin.listTables(path) }

  def add(tableName: String, data: Map[String, Any]): Unit = {
    val document: Document = MapRDB.newDocument()
    data foreach ((entry) => document.set(entry._1, entry._2.toString))
    getOrCreateTable(tableName).insert(UUID.randomUUID().toString(), document)
  }

  def findAll(tablePath: String): Unit = {
    val table: Table = MapRDB.getTable(tablePath)
    val documentStream = table.find()
    for (document <- documentStream) {
      println(document)
    }
  }

  def find(tablePath: String, condition: QueryCondition): Unit = {

    MapRDB.newCondition()
      .and()
      .is("first_name", Op.EQUAL, "Mike")
      .is("release_date", Op.GREATER_OR_EQUAL, new ODate(new java.util.Date("2015-01-01")))
      .close()
      .build()

    val table: Table = MapRDB.getTable(tablePath)
    val documentStream = table.find(condition)
    for (document <- documentStream) {
      println(document)
    }
  }
}