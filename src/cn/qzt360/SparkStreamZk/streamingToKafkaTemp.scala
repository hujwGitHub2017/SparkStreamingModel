package cn.qzt360.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.Properties
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import kafka.utils.ZKGroupTopicDirs
import kafka.common.TopicAndPartition
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.InputDStream
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.HasOffsetRanges
import kafka.utils.ZkUtils
import java.util.Date
import kafka.consumer.SimpleConsumer
import kafka.api.TopicMetadataRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.api.OffsetRequest
import org.apache.zookeeper.CreateMode

/**
 * 最新 spark streaming读取kafka程序
 */
object streamingToKafkaTemp {
  def main(args: Array[String]): Unit = {
    val (brokers, topics) = ("192.168.10.12:9092", "hanytest67")
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafka").set("spark.task.maxFailures", "1")
    val ssc = new StreamingContext(sparkConf, Seconds(40))

    val topic2: String = "hanytest67" //消费的 topic 名字
    val topics2: Set[String] = Set(topic2) //创建 stream 时使用的 topic 名字集合
    val myconf = new MyConf("hyj", topic2)
    //val topicDirs = new ZKGroupTopicDirs("hyj", topic2) //创建一个 ZKGroupTopicDirs 对象，对保存
    val zkTopicPath = myconf.zkTopicPath //获取 zookeeper 中的路径，这里会变成 /consumers/test_spark_streaming_group/offsets/topic_name

    val zkClient = myconf.zkClient //new ZkClient("192.168.10.10:2181") //zookeeper 的host 和 ip，创建一个 client

    var children = zkClient.countChildren(zkTopicPath) //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）

    var kafkaStream: InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map() //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    var kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    kafkaParams += ("metadata.broker.list" -> brokers)
    kafkaParams += ("key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer");
    kafkaParams += ("value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer");
    kafkaParams += ("bootstrap.servers" -> "192.168.10.12:9092")
    kafkaParams += ("request.required.acks" -> "1")
    kafkaParams += ("producer.type" -> "async")
    kafkaParams += ("group.id" -> "hanyingjun")
    if (children == 0) {
      //手工创建分区
      for (i <- 0 to 3) {
        val zkPath = s"${zkTopicPath}/${i}"
        //目的是加载历史数据(对于之前已经传过来的数据 也进行消费)
        ZkUtils.updatePersistentPath(myconf.zkClient, zkPath, "0")
      }
      children = 4
    }
    //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
    //先进行判断offset是否过期
    val topicto = List(topic2)
    val req = new TopicMetadataRequest(topicto, 0)
    val getLeaderConsumer = new SimpleConsumer("192.168.10.12", 9092, 10000, 10000, "OffsetLookup") // 第一个参数是 kafka broker 的host，第二个是 port
    val res = getLeaderConsumer.send(req)
    val topicMetaOption = res.topicsMetadata.headOption
    val partitions = topicMetaOption match {
      case Some(tm) =>
        tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String] // 将结果转化为 partition -> leader 的映射关系
      case None =>
        Map[Int, String]()
    }
    for (i <- 0 until children) {
      val partitionOffset = zkClient.readData[String](s"${zkTopicPath}/${i}")
      val tp = TopicAndPartition(topic2, i)
      val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
      val consumerMin = new SimpleConsumer(partitions.get(i).get, 9092, 10000, 10000, "getMinOffset") //注意这里的 broker_host，因为这里会导致查询不到，解决方法在下面
      val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
      var nextOffset = partitionOffset.toLong
      if (curOffsets.length > 0 && nextOffset < curOffsets.head) { // 通过比较从 kafka 上该 partition 的最小 offset 和 zk 上保存的 offset，进行选择
        nextOffset = curOffsets.head
      }
      fromOffsets += (tp -> nextOffset) //设置正确的 offset，这里将 nextOffset 设置为 0（0 只是一个特殊值），可以观察到 offset 过期的现象  
    }
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.offset + "\t" + mmd.partition + "\t" + mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
    kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    var offsetRanges = Array[OffsetRange]()

    kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.foreachRDD { rdd =>
      val myconf = new MyConf("hyj", topic2)
      for (o <- offsetRanges) {
        val zkPath = s"${zkTopicPath}/${o.partition}"
        println("firstage1=" + o.partition + "------" + o.fromOffset.toString)
        ZkUtils.updatePersistentPath(myconf.zkClient, zkPath, o.fromOffset.toString) //将该 partition 的 offset 保存到 zookeeper
      }
      val time = new Date

      rdd.foreachPartition { x =>
        {
          val myconf2 = new MyConf("hyj", topic2)
          var tempUpdate = ""
          var isRun = true
          while (x.hasNext && isRun) {
            val temps = x.next()
            if (!x.hasNext) {
              //获取最后一个offset
              tempUpdate = temps._2
            }
            val temp = temps._2
            println("test0001==" + temps._1 + "\t" + temp)
            try {
              val te = temp.split("\\t")
              if (te(2).equals("abc100")) {
                //throw new RuntimeException
              }
            } catch {
              case es: Exception => {
                //此时进行重新赋值zookeeper
                val ttmps = temp.split("\\t")
                val offset = ttmps(0)
                val part = ttmps(1)
                val zkTopicPath = myconf2.zkTopicPath
                val zkPath = s"${zkTopicPath}/${part}"
                println("errorOffset=" + part + "------" + offset)
                ZkUtils.updatePersistentPath(myconf2.zkClient, zkPath, offset) //将该 partition 的 offset 保存到 zookeeper
                myconf2.zkClient.create("/sparkStreamingError" + part, "exception", CreateMode.PERSISTENT)
                isRun = false
              }
            }
            //更新offset
            if (tempUpdate.length() > 0) {
              val ttmps = tempUpdate.split("\\t")
              val offset = (ttmps(0).toLong + 1).toString()
              val part = ttmps(1)

              val myconf3 = new MyConf("hyj", topic2)

              val zkTopicPath = myconf3.zkTopicPath
              val zkPath = s"${zkTopicPath}/${part}"
              ZkUtils.updatePersistentPath(myconf3.zkClient, zkPath, offset) //将该 partition 的 offset 保存到 zookeeper 
            }
          }
        }
      }
      //查看zk是否有目录 如果有 则退出
      //val myconf2 = new MyConf("hyj", topic2)
      var isdele = false
      for (x <- 0 to 4) {
        val isExixt = myconf.zkClient.exists("/sparkStreamingError" + x)
        if (isExixt) {
          isdele = true
        }
      }
      if (isdele) {
        for (x <- 0 to 4) {
          val isExixt = myconf.zkClient.exists("/sparkStreamingError" + x)
          if (isExixt) {
            myconf.zkClient.deleteRecursive("/sparkStreamingError" + x)
          }
        }
        throw new RuntimeException("--------------数据导致程序报错--------------")
      }

      //下面就可以根据rdd写具体的业务逻辑了  下面的save作为调试用
      //rdd.repartition(1).saveAsTextFile("/user/qzt_java/myhanyingjun/test/" + time.getTime)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}