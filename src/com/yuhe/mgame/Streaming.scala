package com.yuhe.mgame

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.{ Duration, StreamingContext, Seconds }
import org.apache.spark.streaming.kafka._
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import kafka.utils.ZkUtils
import kafka.utils.ZKGroupTopicDirs

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.consumer.SimpleConsumer
import kafka.api.TopicMetadataRequest
import collection.mutable.{ Map => MutableMap }

import com.alibaba.fastjson.{ JSON, JSONObject }
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.{ Level, Logger }
import com.yuhe.mgame.db.ServerDB
import com.yuhe.mgame.log._

object Streaming {
  Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  val groupID = "logstash_consumer"
  val topic = "logstash"
  val zkClient = new ZkClient("localhost:2181", 60000, 60000, new ZkSerializer {
    override def serialize(data: Object): Array[Byte] = {
      try {
        return data.toString().getBytes("UTF-8")
      } catch {
        case e: ZkMarshallingError => return null

      }
    }
    override def deserialize(bytes: Array[Byte]): Object = {
      try {
        return new String(bytes, "UTF-8")
      } catch {
        case e: ZkMarshallingError => return null
      }
    }
  })
  val topicDirs = new ZKGroupTopicDirs("xl_streaming", topic)

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("streaming")
    val scc = new StreamingContext(conf, Duration(5000))
    //    scc.checkpoint(".") // 因为使用到了updateStateByKey,所以必须要设置checkpoint
    val topics = Set(topic) //我们需要消费的kafka数据的topic
    val kafkaParam = Map(
      "metadata.broker.list" -> "localhost:9092", // kafka的broker list地址
      "group.id" -> groupID,
      "auto.offset.reset" -> "largest")

    var kafkaStream: InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    if (children > 0) {
      //---get partition leader begin----    
      val topicList = List(topic)
      val req = new TopicMetadataRequest(topicList, 0) //得到该topic的一些信息，比如broker,partition分布情况    
      val getLeaderConsumer = new SimpleConsumer("localhost", 9092, 10000, 10000, "OffsetLookup") // brokerList的host 、brokerList的port、过期时间、过期时间   
      val res = getLeaderConsumer.send(req) //TopicMetadataRequest   topic broker partition 的一些信息    
      val topicMetaOption = res.topicsMetadata.headOption
      val partitions = topicMetaOption match {
        case Some(tm) =>
          tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None =>
          Map[Int, String]()
      }
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        //---additional begin-----    
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))) // -2,1    
        val consumerMin = new SimpleConsumer(partitions(i), 9092, 10000, 10000, "getMinOffset")
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        var nextOffset = partitionOffset.toLong
        if (curOffsets.length > 0 && nextOffset < curOffsets.head) { //如果下一个offset小于当前的offset    
          nextOffset = curOffsets.head
        }
        //---additional end-----    
        fromOffsets += (tp -> nextOffset)
        fromOffsets += (tp -> partitionOffset.toLong) //将不同 partition 对应的 offset 增加到 fromOffsets中 
      }
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple  
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](scc, kafkaParam, fromOffsets, messageHandler)
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
    }

    //统计专区和sdkMap都放到广播变量当中
    var staticsServers = BroadcastWrapper[scala.collection.mutable.Map[String, ArrayBuffer[String]]](scc, ServerDB.getStaticsServers)
    val broadSDKMap = BroadcastWrapper[scala.collection.mutable.Map[String, String]](scc, ServerDB.getSDKMap)
    var lastTime = System.currentTimeMillis
    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD(rdd => {
      if (!rdd.isEmpty) { //判断是否为空，防止空的rdd也在触发计算
        if (System.currentTimeMillis - lastTime > 300000) {
          //超过5分钟，更新广播变量
          lastTime = System.currentTimeMillis
          staticsServers.update(ServerDB.getStaticsServers, true)
          broadSDKMap.update(ServerDB.getSDKMap, true)
        }
        rdd.foreachPartition(partitionOfRecords => {
          val logMap = MutableMap[String, ArrayBuffer[JSONObject]]()
          partitionOfRecords.foreach(record => {
            val json = JSON.parseObject(record._2)
            val logType = json.getOrDefault("type", "").toString
            logMap(logType) = logMap.getOrElse(logType, ArrayBuffer[JSONObject]())
            logMap(logType) += json
          })
          for((logType, jsonList) <- logMap){
            val logObject = getLogObject(logType)
            if (logObject != null) {
              val serverMap = staticsServers.value
              val sdkMap = broadSDKMap.value
              logObject.parseLog(jsonList, serverMap, sdkMap)
            }
          }
        })
      }
      for (o <- offsetRanges) {
        ZkUtils.updatePersistentPath(zkClient, s"${topicDirs.consumerOffsetDir}/${o.partition}", o.fromOffset.toString)
      }
    })

    scc.start() // 真正启动程序
    scc.awaitTermination() //阻塞等待
  }

  def getLogObject(logType: String) = {
    val classMap = Map(
      "login" -> LoginLog,
      "logout" -> LogoutLog,
      "addplayer" -> AddPlayerLog,
      "online" -> Online)
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