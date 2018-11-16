package com.uhg.mapr.db

import com.mapr.db.{ Admin, Table, MapRDB }
import org.ojai.Document
import org.apache.hadoop.fs.Path
import java.util.UUID
import com.uhg.mapr.context.Context

object MaprDBJSON extends Context with MapRDBCommon {

  val admin: Admin = MapRDB.newAdmin();

  def getOrCreateTable(tableName: String): Table = {
    if (!MapRDB.tableExists(tableName)) {
      admin.createTable(new Path(tableName));
    } else {
      MapRDB.getTable(tableName);
    }
  }

  def deletTable(tableName: String): Boolean = {
    if (!MapRDB.tableExists(tableName))
      admin.deleteTable(new Path(tableName));
    false
  }

  def getTables(): java.util.List[Path] = { admin.listTables() }

  def add(table: Table, data: Map[String, Any]): Unit = {
    val document: Document = MapRDB.newDocument();
    data foreach ((entry) => document.set(entry._1, entry._2.toString))
    table.insert(UUID.randomUUID().toString(), document);
  }
}