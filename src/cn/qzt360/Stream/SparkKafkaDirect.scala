package cn.qzt360.Stream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.HasOffsetRanges

object SparkKafkaDirect {
  
  def main(args: Array[String]): Unit = {
    
    
    val conf = new SparkConf().setAppName("SparkStreamingModel")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    
    val sc =  new SparkContext(conf)
    
    val jssc = new StreamingContext(sc,Seconds(20))
    
       
     //kafka参数设置  metadata.broker.list
    val kafkaParams:Map[String,String] = Map("metadata.broker.list" ->"datanode09:9092,datanode10:9092","group.id" -> "htest")
      
    
     //创建一个set，里面放入，你要读取的topic.  可以并行读取多个topic
    
    val topics:Set[String] = Set("ba_authlog2016_ok","wjpt_http_src")
    
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](jssc, kafkaParams, topics) 
    
    
    messages.foreachRDD(rdd =>{
      rdd.foreach(f =>{
        println("1、 "+f._1+"  "+"2、 "+f._2)
      })
    })
    
//    val mes = messages.map(lines => lines._2)
//    
//    mes.foreachRDD(rdd =>{
//      rdd.foreach(f => println(""))
//    })
    
    //对数据进行处理
//    val info = messages.map(lines => (lines._1))
//    
//    info.foreachRDD(rdd =>{
//      rdd.foreach { x => println("info ==========="+x)}
//    })
    
//    messages.foreachRDD(rdd =>{
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      //offsetRanges(0).fromOffset
//      offsetRanges.foreach { x => println("ofset============"+x.fromOffset)}
//      }
//    )
    //println("data==================="+messages.count())
    
//    Thread.sleep(5000)
//    lines.print() cn.spark.Streaming.SparkStreamingCheckPoint
    
   /* var num:Long = 0;
    
    lines.foreachRDD(rdd =>{
      
      num = num + rdd.count()
      
    	println("num = "+ num)
      
    })
    
    Thread.sleep(5000)
    
    lines.saveAsTextFiles("/user/hujw/sparkdata/0824", "ok")*/
    
//    lines.saveAsHadoopFiles("/user/hujw/sparkdata/0824", "ok")
    
    // Start the computation
    jssc.start()
    jssc.awaitTermination()
    
    
  }
  
}