package com.uhg.mapr.config

import com.typesafe.config.Config
import scala.collection.JavaConversions._
import com.typesafe.config.Config
import com.uhg.mapr.context.Context

object ConfigTest extends Context with App {
  
  fromConfigMap

  def fromMap(): Unit = {
    val data = Map(
      "key1" -> 3.0,
      "key2" -> 4.0,
      "key3" -> 3.5)
    data foreach ((entry) => println(entry._1 + "," + entry._2.toString))
  }

  def fromConfigMap(): Unit = {
    val maps: java.util.List[_ <: com.typesafe.config.Config] = config.getConfigList("conf.task.handlers")
    for (map <- maps) {
      println(map.entrySet.foreach(p => println(p.getKey + " : " + p.getValue.unwrapped)))
    }
  }
}
