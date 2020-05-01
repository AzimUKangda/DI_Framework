package com.test.framework.config

import com.test.framework.common.Utility

abstract class GenericConfig {

  /**
    * This method returns the attribute of the class as a Map
    * In Some cases there are services that needs the configuration in a Map Format
    * Currently it only consider the attributes of type String
    */
  def attributesToMap(): Map[String, String] = {
    var attributesMap = Map[String, String]()
    getClass.getDeclaredFields.foreach(f => {
      if (f.getType.getSimpleName == "String") {
        attributesMap += (Utility
          .camelToPointCase(f.getName) -> getClass.getMethods
          .find(_.getName == f.getName)
          .get
          .invoke(this).asInstanceOf[String])
      }
    })
    attributesMap
  }

}
