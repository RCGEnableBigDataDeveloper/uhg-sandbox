package com.uhg.mapr.db

import com.uhg.mapr.context.Context
import org.apache.hadoop.fs.Path
import scala.collection.JavaConversions._
import org.scalatest._

object MaprDBJSONTest extends Context with App {

  val mapr = new MaprDBJSON

  val tableName: String = "/user/eperler/segmentation_db_json"

  getMessage
  getTables
  add
  findAll

  def getMessage(): Unit = {
    val map: Map[String, Any] = mapr.parse()
    println(map.get("type").get)

  }

  def getTables(): Unit = {
    val tables: java.util.List[Path] = mapr.getTables("/user/eperler/")
    for (table <- tables) {
      println(table)
    }
  }

  def add(): Unit = {
    val data = Map(
      "key1" -> "{\"first_name\":\"John\", \"last_name\":\"Doe\", \"age\" : 34 }",
      "key2" -> "{\"first_name\":\"Mike\", \"last_name\":\"Smith\", \"age\" : 43 }",
      "key3" -> "{\"first_name\":\"Paul\", \"last_name\":\"Rogers\", \"age\" : 52 }")

    mapr.add(tableName, data)
  }

  def findAll(): Unit = {
    mapr.findAll(tableName)
  }
}