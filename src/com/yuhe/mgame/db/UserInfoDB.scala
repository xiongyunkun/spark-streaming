package com.yuhe.mgame.db

import scala.collection.mutable.ArrayBuffer
import collection.mutable.{Map => MutableMap}
import java.text.SimpleDateFormat
import java.util.Date

object UserInfoDB {
  /**
   * 根据登陆日志插入玩家角色信息表
   */
  def insertLoginUser(platformID:String, results:ArrayBuffer[MutableMap[String, String]]): Unit = {
    val insertCols = Array("HostID", "Uid", "Urs", "Name", "Time", "LastUpdateTime", "SrcHostID")
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timeStr = timeFormat.format(new Date())
    val sqlValues = ArrayBuffer[String]()
    for(result <- results){
      val values = ArrayBuffer[String]()
      for(col <- insertCols){
        var value = result.getOrElse(col, "")
        if(col == "LastUpdateTime"){
          value = timeStr
        }
        values += value
      }
      sqlValues += values.mkString("','")
    }
    //数据库要记录的列
    val cols = Array("HostID", "Uid", "Urs", "Name", "LastLoginTime", "LastUpdateTime", "SrcHostID")
    val sql = "insert into ".concat(platformID).concat("_statics.tblUserInfo(").concat(cols.mkString(","))
				.concat(") values('").concat(sqlValues.mkString("'),('"))
				.concat("') on duplicate key update Name = values(Name),LastLoginTime=values(LastLoginTime),LastUpdateTime=values(LastUpdateTime),OnlineFlag='1'")
//  	println(sql)
	  DBManager.insert(sql)
  }
  /**
   * 根据登出日志插入玩家角色信息表
   */
  def insertLogoutUser(platformID:String, results:ArrayBuffer[MutableMap[String, String]]) = {
    val updateCols = Array("HostID", "Uid", "Urs", "Name", "Level", "LastLogoutTime", "TotalOnlineTime", "Gold",
				"Money", "OnlineFlag", "LastUpdateTime", "SrcHostID")
		val duplicateCols = Array("Name", "Urs", "Level", "LastLogoutTime", "Gold", "Money", "OnlineFlag",
				"LastUpdateTime", "SrcHostID")
		val colMap = Map("LastLogoutTime" -> "Time", "TotalOnlineTime" -> "OnTime")
		val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timeStr = timeFormat.format(new Date())
    val defaultValues = Map("OnlineFlag" -> "0", "LastUpdateTime" -> timeStr)
    val sqlValues = ArrayBuffer[String]()
    for(result <- results){
      val values = ArrayBuffer[String]()
      for(col <- updateCols){
        var value = result.getOrElse(col, "")
        if (colMap.contains(col)) {
					value = result.getOrElse(colMap(col), "")
				} else if (defaultValues.contains(col)) {
					value = defaultValues(col)
				}
				values += value
      }
      sqlValues += values.mkString("','")
    }
    val duplicates = ArrayBuffer[String]()
    for(col <- duplicateCols){
      duplicates += col.concat("=values(").concat(col).concat(")")
    }
    val sql = "insert into ".concat(platformID).concat("_statics.tblUserInfo(")
				.concat(updateCols.mkString(",")).concat(") values('")
				.concat(sqlValues.mkString("'),('")).concat("') on duplicate key update ")
				.concat(duplicates.mkString(","))
				.concat(",TotalOnlineTime = TotalOnlineTime + values(TotalOnlineTime)")
//		println(sql)
		DBManager.insert(sql)
  }
  /**
   * 根据创角日志插入玩家角色信息表
   */
  def insertAddPlayerUser(platformID:String, results:ArrayBuffer[MutableMap[String, String]]) = {
    val nameCols = Array( "HostID", "Uid", "Urs", "Time", "SrcHostID")
    val sqlValues = ArrayBuffer[String]()
    for(result <- results){
      val values = ArrayBuffer[String]()
      for(nameCol <- nameCols){
        val value = result.getOrElse(nameCol, "")
        values += value
      }
      sqlValues += values.mkString("','")
    }
    val cols = Array("HostID", "Uid", "Urs", "RegTime", "SrcHostID")
    val sql = "insert into ".concat(platformID).concat("_statics.tblUserInfo(")
				.concat(cols.mkString(",")).concat(") values('").concat(sqlValues.mkString("'),('"))
				.concat("') on duplicate key update RegTime = values(RegTime)")
//		println(sql)
		DBManager.insert(sql)
  }
}