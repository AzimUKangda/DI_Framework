package com.test.framework.common

import scala.util.parsing.json.{Parser => JSONParser, JSONObject, JSONArray}

class JSonParser extends JSONParser{

  private def resolveType(input:Any):Any = input match {
    case JSONObject(map) => map.transform{case (k,v) => resolveType(v)}
    case JSONArray(seq) => seq.map(resolveType)
    case other => other
  }

  def parse(input:String):Option[Any] = {
    phrase(root)(new lexical.Scanner(input)) match {
      case Success(result,_) => Some(resolveType(result))
      case _ => None
    }
  }
}

object JSonParser{
  def parse(input:String):Option[Any] = (new JSonParser).parse(input)
}
