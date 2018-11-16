package com.uhg.mapr.context

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

class Context {
  
  val config: Config = ConfigFactory.load()
  
}