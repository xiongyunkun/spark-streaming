package com.yuhe.mgame

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.{ Duration, StreamingContext, Seconds }
import org.apache.spark.streaming.kafka.KafkaUtils

import com.alibaba.fastjson.{ JSON, JSONObject }
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.{ Level, Logger }
import com.yuhe.mgame.db.ServerDB
import com.yuhe.mgame.log._

object Streaming {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("streaming")
    val scc = new StreamingContext(conf, Duration(5000))
    //    scc.checkpoint(".") // 因为使用到了updateStateByKey,所以必须要设置checkpoint
    val topics = Set("logstash") //我们需要消费的kafka数据的topic
    val kafkaParam = Map(
      "metadata.broker.list" -> "192.168.1.97:9092" // kafka的broker list地址
      )
    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topics)
    val logDStream = stream.map(_._2).map(line => {
      val json = JSON.parseObject(line)
      val logType = json.getOrDefault("type", "")
      (logType, json)
    }).filter(x => !x._1.equals("")).groupByKey
    //统计专区和sdkMap都放到广播变量当中
    var staticsServers = BroadcastWrapper[scala.collection.mutable.Map[String, String]](scc, ServerDB.getStaticsServers)
    val broadSDKMap = BroadcastWrapper[scala.collection.mutable.Map[String, String]](scc, ServerDB.getSDKMap)
    var lastTime = System.currentTimeMillis
    logDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty) { //判断是否为空，防止空的rdd也在触发计算
        if (System.currentTimeMillis - lastTime > 300000) {
          //超过5分钟，更新广播变量
          lastTime = System.currentTimeMillis
          staticsServers.update(ServerDB.getStaticsServers, true)
          broadSDKMap.update(ServerDB.getSDKMap, true)
        }
        rdd.foreachPartition(partitionOfRecords => {
          partitionOfRecords.foreach(record => {
            val (logType: String, logList: ArrayBuffer[JSONObject]) = record
            val logObject = getLogObject(logType)
            if (logObject != null) {
              val serverMap = staticsServers.value
              val sdkMap = broadSDKMap.value
              logObject.parseLog(logList, serverMap, sdkMap)
            }
          })
        })
      }
    })

    scc.start() // 真正启动程序
    scc.awaitTermination() //阻塞等待
  }

  def getLogObject(logType: String) = {
    val classMap = Map(
      "login" -> LoginLog,
      "logout" -> LogoutLog,
      "addplayer" -> AddPlayerLog)
    if (classMap.contains(logType))
      classMap(logType)
    else
      null
  }

  /**
   * 创建一个从kafka获取数据的流.
   * @param scc           spark streaming上下文
   * @param kafkaParam    kafka相关配置
   * @param topics        需要消费的topic集合
   * @return
   */
  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }
}

import java.io.{ ObjectInputStream, ObjectOutputStream }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import scala.reflect.ClassTag

case class BroadcastWrapper[T: ClassTag](
    @transient private val ssc: StreamingContext,
    @transient private val _v: T) {

  @transient private var v = ssc.sparkContext.broadcast(_v)

  def update(newValue: T, blocking: Boolean = false): Unit = {
    // 删除RDD是否需要锁定
    v.unpersist(blocking)
    v = ssc.sparkContext.broadcast(newValue)
  }

  def value: T = v.value

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[T]]
  }
}