package cn.spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder

object SparkStreamingCheckPoint {
  
  //构建streamingContext
  def functionToCreateContext():StreamingContext={
    
    //初始化
    val conf = new SparkConf().setAppName("Kafka")
    val ssc = new StreamingContext(conf,Seconds(5))
    //设置kafka
    //kafka参数设置  metadata.broker.list
    val kafkaParams:Map[String,String] = Map("metadata.broker.list" ->"192.168.10.12:9092,192.168.10.13:9092,192.168.10.14:9092","group.id" -> "checkpoint")
    val topics:Set[String] = Set[String]("flumetopic02")
    val dataStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)//.createDirectStream[](ssc, kafkaParams, topics)
    //对数据进行处理
    ssc.checkpoint("/user/hujwtest/checkoint2")
    //设置通过间隔时间，定时持久checkpoint到hdfs上
    dataStream.checkpoint(Seconds(5))
    dataStream.foreachRDD(rdd =>{
      rdd.foreach { x => println("datainfo ==========="+x._1+">>"+x._2)}
    })
    
    dataStream.foreachRDD(rdd =>{
      //获取kafka中数据的信息
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //offsetRanges(0).fromOffset
      offsetRanges.foreach { x => println("ofset============"+x.fromOffset)}
      val offsetsRangesStr = offsetRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
      println("data info :"+offsetsRangesStr)
       //ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)  // 用zookeeper保存信息
      }
    )
    
    //对元数据信息进行保存
    ssc
  }
 
 def main(args: Array[String]): Unit = {
   // 创建context
  val context = StreamingContext.getOrCreate("/user/hujwtest/checkoint2",functionToCreateContext)
  // 启动流计算
  context.start()
  context.awaitTermination()
 }
  
}