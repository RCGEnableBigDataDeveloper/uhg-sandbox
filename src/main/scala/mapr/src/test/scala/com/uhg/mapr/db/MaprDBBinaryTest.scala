package com.uhg.mapr.db

import com.uhg.mapr.context.Context
import org.apache.hadoop.fs.Path
import scala.collection.JavaConversions._
import org.scalatest._
import java.util.UUID

object MaprDBBinaryTest extends Context with App {

  val mapr = new MaprDBBinary

  val tableName: String = "/user/eperler/segmentation_db_json"

  getTables
  add
  findAll

  def getTables(): Unit = {
    val tables: java.util.List[String] = mapr.getTables()
    for (table <- tables) {
      println(table)
    }
  }

  def add(): Unit = {
    val data = Map(
      "key1" -> "{\"first_name\":\"John\", \"last_name\":\"Doe\", \"age\" : 34 }",
      "key2" -> "{\"first_name\":\"Mike\", \"last_name\":\"Smith\", \"age\" : 43 }",
      "key3" -> "{\"first_name\":\"Paul\", \"last_name\":\"Rogers\", \"age\" : 52 }")

    mapr.add(UUID.randomUUID().toString, "cf", tableName, data)
  }

  def findAll(): Unit = {
    mapr.findAll(tableName)
  }
}