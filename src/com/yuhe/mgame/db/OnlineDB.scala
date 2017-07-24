package com.yuhe.mgame.db

import scala.collection.mutable.ArrayBuffer
import org.apache.commons.lang.StringUtils

object OnlineDB {

  val DB_COLS = Array("HostID", "OnlineNum", "Time", "PlatformID")

  def insert(platformID: String, results: ArrayBuffer[Map[String, String]]) = {
    val sqlValues = ArrayBuffer[String]()
    for (result <- results) {
      val values = ArrayBuffer[String]()
      for (col <- DB_COLS) {
        values += result(col)
      }
      sqlValues += values.mkString("','")
    }
    val sql = "insert into ".concat(platformID).concat("_statics.tblOnline(").concat(DB_COLS.mkString(","))
      .concat(") values('").concat(sqlValues.mkString("'),('"))
      .concat("') on duplicate key update OnlineNum = values(OnlineNum)")
    DBManager.insert(sql)
  }
}