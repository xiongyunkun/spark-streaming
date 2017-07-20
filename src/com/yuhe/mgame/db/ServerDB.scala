package com.yuhe.mgame.db

import scala.collection.mutable.{Map => MutableMap}

object ServerDB {
  /**
   * 获得(sdkID => platformID)键值对
   */
  def getSDKMap() = {
    val sdkMap = MutableMap[String, String]()
    val sql = "select * from smcs.tblPlatform where Flag = 'true'"
    val conn = DBManager.getConnection
    try{
      val smst = conn.createStatement
      val results = DBManager.query(smst, sql)
      while(results.next){
        val sdkID = results.getString("SDKID")
        val platformID = results.getString("PlatformID")
        sdkMap(sdkID) = platformID
      }
    }catch{
      case ex: Exception =>
        ex.printStackTrace()
    }
    sdkMap
  }
  /**
   * 获得统计服(hostID => platformID)
   */
  def getStaticsServers() = {
    val serverMap = MutableMap[String, String]()
    var sql = "select a.serverid as HostID, c.platformid as PlatformID from smcs.srvgroupinfo a, "
    sql += "smcs.servergroup b, smcs.servers c where a.groupid = b.id and b.name = '统计专区' and a.serverid = c.hostid"
    val conn = DBManager.getConnection
    try{
      val smst = conn.createStatement
      val results = DBManager.query(smst, sql)
      while(results.next){
        val hostID = results.getString("HostID")
        val platformID = results.getString("PlatformID")
        serverMap(hostID) = platformID
      }
    }catch{
      case ex: Exception =>
        ex.printStackTrace()
    }
    serverMap
  }
}