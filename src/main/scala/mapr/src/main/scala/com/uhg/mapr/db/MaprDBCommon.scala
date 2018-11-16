package com.uhg.mapr.db

import scala.util.parsing.json.JSON
import java.io.IOException

trait MapRDBCommon {

  def parse(): Map[String, Any] = {
    val payload = io.Source.fromInputStream(getClass.getResourceAsStream("/msg.json")).mkString
    val result = JSON.parseFull(payload)
    result match {
      case Some(map: Map[String, Any]) =>  map
      case None => throw new IOException("parsing failed")
      case _ => throw new IOException("unknown data structure")
    }
  }
}