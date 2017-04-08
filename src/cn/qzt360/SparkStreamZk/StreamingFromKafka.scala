package cn.qzt360.SparkStreamZk

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.utils.ZKGroupTopicDirs
import kafka.api.TopicMetadataRequest
import kafka.consumer.SimpleConsumer
import kafka.utils.ZkUtils
import kafka.common.TopicAndPartition
import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaCluster
import com.database.JobCmd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.Set
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD

/**
 *
 * createDirectStream   方法创建出来的dstream的rdd partition和kafka的topic的partition是一一对应
 */
object StreamingFromKafka {

  def main(args: Array[String]): Unit = {

    val topic = "kk_jingxin_src"
    val groupId = "count"
    val hdfsConf: Configuration = new Configuration()
    var fs: FileSystem = null
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(120))
    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "192.168.10.12:9092,192.168.10.13:9092")
    val topics = scala.collection.immutable.Set(topic)
    var kafkaStream: InputDStream[(String, String)] = null
    val myzkConf = new zkConf(groupId, topic)
    val zkClient = myzkConf.zkClient
    var children = zkClient.countChildren(myzkConf.zkTopicPath);
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    //这部操作 解决重启程序的时候需要从kafka的开始多
    /*if (children == 0) {
        //第一次消费时,zk没有数据，手工创建分区
        for (i <- 0 to 7) {
          val zkPath = s"${myzkConf.zkTopicPath}/${i}"
          //目的是加载历史数据(对于之前已经传过来的数据 也进行消费)
          ZkUtils.updatePersistentPath(zkClient, zkPath, "0")
        }
        children = 8
       }*/
    if (children > 0) {
      //---get partition leader begin----    
      val topicList = List(topic)
      val req = new TopicMetadataRequest(topicList, 0) //得到该topic的一些信息，比如broker,partition分布情况
      val getLeaderConsumer = new SimpleConsumer("192.168.10.12", 9092, 10000, 10000, "OffsetLookup") // brokerList的host 、brokerList的port、过期时间、过期时间
      val res = getLeaderConsumer.send(req) //TopicMetadataRequest   topic broker partition 的一些信息
      val topicMetaOption = res.topicsMetadata.headOption
      val partitions = topicMetaOption match {
        case Some(topicMeta) =>
          topicMeta.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None =>
          Map[Int, String]()
      }

      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${myzkConf.zkTopicPath}/${i}") // zk上的offset
        val tp = TopicAndPartition(topic, i)
        //---additional begin----- 
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        val consumerMin = new SimpleConsumer(partitions.get(i).get, 9092, 10000, 10000, "getMinOffset") //注意这里的 broker_host，因为这里会导致查询不到，解决方法在下面
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        var nextOffset = partitionOffset.toLong
        if (curOffsets.length > 0 && nextOffset < curOffsets.head) { // 通过比较从 kafka 上该 partition 的最小 offset 和 zk 上保存的 offset，进行选择
          nextOffset = curOffsets.head
        }
        fromOffsets += (tp -> nextOffset) //设置正确的 offset，   这里将 nextOffset 设置为 0（0 只是一个特殊值），可以观察到 offset 过期的现象  
      }

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.message(), mmd.topic + "\t" + mmd.offset + "\t" + mmd.partition) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }
    var offsetRanges = Array[OffsetRange]()
    val infoRdd = kafkaStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    infoRdd.foreachRDD(rdd => {
      //操作zookeeper保存offset
      val myzkConf = new zkConf(groupId, topic)
      for (o <- offsetRanges) {
        val zkPath = s"${myzkConf.zkTopicPath}/${o.partition}"
        println("firstage1=" + o.partition + "------" + o.fromOffset.toString)
        ZkUtils.updatePersistentPath(myzkConf.zkClient, zkPath, o.fromOffset.toString) //将该 partition 的 offset 保存到 zookeeper
      }
      //获取数据库的设备信息
      val deviceInfo = JobCmd.getMonitordevice()
      deviceInfo match {
        case Some(device) => {
          //获取hdfs上的文件
          var monitorAreaInfo: RDD[String] = null
          fs = FileSystem.newInstance(hdfsConf)
          val monitorAreaInfofilePathSet = getHdfsFile(fs, "/user/hujw/monitorArea")
          if (monitorAreaInfofilePathSet.size > 0) {
            val monitorAreaInfofilePath = monitorAreaInfofilePathSet.mkString(",")
            println("hdfs File path ==============  " + monitorAreaInfofilePath)
            monitorAreaInfo = sc.textFile(monitorAreaInfofilePath)
          }
          var dataTopic: String = ""
          var dataOffset: String = ""
          var dataPartition: String = ""
          var dataMessage: String = ""
          try {
            //正常的数据处理逻辑
            rdd.foreachPartition(p => {
              while (p.hasNext) {
                val infoTemp = p.next()
                //获取到的正常数据
                dataMessage = infoTemp._1
                dataTopic = infoTemp._2.split("\t")(0)
                dataOffset = infoTemp._2.split("\t")(1)
                dataPartition = infoTemp._2.split("\t")(2)
                print("info = " + infoTemp)
              }
            })

            if (monitorAreaInfo != null) {
              monitorAreaInfo.foreach(f => {
                println("hdfsinfo = " + f)
              })
            } else {
              println("hdfsinfo = null")
            }
            //   测试读取数据--------------------------------------------
            for (driveId <- device.keySet) {
              println("driveId = " + driveId)
              println(" area info = " + device.get(driveId).get.asInstanceOf[scala.collection.mutable.Map[String, Any]].get("area_id").get.asInstanceOf[String])
            }
            //在保存新文件的时候，将旧文件删除
            delHdfsFile(fs, monitorAreaInfofilePathSet)
            fs.close()

          } catch {
            case e: Exception => {
              if (dataOffset.length() > 0) {
                val myZkConf = new zkConf(groupId, dataTopic)
                val zkTopicPath = myZkConf.zkTopicPath
                val zkPath = s"${zkTopicPath}/${dataPartition}"
                ZkUtils.updatePersistentPath(myZkConf.zkClient, zkPath, dataOffset) //将该 partition 的 offset 保存到 zookeeper
              }
            } // TODO: handle error
          }

          //---------------------------------------------------------------------------------
        }
        case None => {
          println("===========================no CMD ========================")
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def getHdfsFile(fs: FileSystem, filepath: String): Set[String] = {
    val files = Set[String]()
    val fileList: Array[FileStatus] = fs.listStatus(new Path(filepath))
    fileList.foreach(fileStatus => {
      files += fileStatus.getPath.toString()
    })
    files
  }

  def delHdfsFile(fs: FileSystem, filePath: Set[String]) {
    for (file <- filePath) {
      fs.delete(new Path(file), true)
    }
  }
}