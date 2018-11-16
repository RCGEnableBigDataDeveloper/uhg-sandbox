package com.uhg.mapr.db

import com.uhg.mapr.context.Context

class MaprDBTest extends Context with App {

  def fromMap(): Unit = {
    val x = MaprDBJSON
    val map: Map[String, Any] = x.parse()
    println(map.get("type").get)
  }
}