package cn.qzt360.Stream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scala.collection.immutable.Map
import scala.collection.immutable.Set
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.Durations

object StartSparkStreaming {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("SparkStreamingModel")
    
    val sc = new SparkContext(conf) 
    
    val jssc = new StreamingContext(sc,Seconds(20))
    
    
   /* //kafka参数设置  metadata.broker.list
    val kafkaParams:Map[String,String] = Map("metadata.broker.list" ->"datanode01:9092,datanode02:9092,datanode03:9092",
                                             "serializer.class" -> "kafka.serializer.StringEncoder")
    
     //创建一个set，里面放入，你要读取的topic.  可以并行读取多个topic
    
    val topics:Set[String] = Set("TestTopic")
    */
//    val line = KafkaUtils.createDirectStream(jssc, kafkaParams, topics)
    
    
    
    val topics:Map[String,Int] =Map("UploadFile"->1) 
    
    val line = KafkaUtils.createStream(jssc, "192.168.10.10:2181,192.168.10.11:2181,192.168.10.12:2181", "DefaultGroup", topics)

    val values = line.map(v => Some(v._2.toString()))
    
    
        
     var num:Long = 0;
    
    values.foreachRDD(rdd =>{
          
      num = num + rdd.count()
      
      println("num = "+ num)
      
    })
    
    
    Thread.sleep(5000)
    
//    values.print()
        
    values.saveAsTextFiles("/user/hujw/sparkdata/0824", "ok")
    
    jssc.start()
    jssc.awaitTermination()
    
  }
  
}