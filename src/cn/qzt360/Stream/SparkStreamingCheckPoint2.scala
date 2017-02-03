package cn.qzt360.Stream

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreamingCheckPoint2 {
  
  def createContext(checkpointDirectory:String):StreamingContext={
    
		  val sparkConf = new SparkConf().setAppName("KafkaTest")  
      val ssc = new StreamingContext(sparkConf, Seconds(1))
       ssc.checkpoint(checkpointDirectory)
      //kafka参数设置  metadata.broker.list
      val kafkaParams:Map[String,String] = Map("metadata.broker.list" ->"datanode01:9092,datanode02:9092,datanode03:9092","group.id" -> "checkpoint")
      val topics:Set[String] = Set[String]("flumetopic")
      val dataStream = KafkaUtils.createDirectStream(ssc, kafkaParams, topics) 
      
      ssc
  }
  
  
  /**
   * spark streaming 使用 kafka 自定义  offset
   * 
   */
  
  
  /**
  def kafkaStream(...): InputDStream[(String, String)] = {
  ...
  val storedOffsets = readOffsets(...)
  val kafkaStream = storedOffsets match {
    case None =>
      // start from the latest offsets
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    case Some(fromOffsets) =>
      // start from previously saved offsets
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,
        (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
  }
  ...
}
  
  **/
  
  /**
  // Hold a reference to the current offset ranges, so it can be used downstream
 var offsetRanges = Array[OffsetRange]()
	
 directKafkaStream.transform { rdd =>
   offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
   rdd
 }.map {
           ...
 }.foreachRDD { rdd =>
   for (o <- offsetRanges) {
     println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
   }
   ...
 }
  **/
}