package cn.qzt360.Stream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Sparkhdfs {
  
  def main(args: Array[String]): Unit = {
    
     val conf = new SparkConf().setAppName("SparkhdfsModel")
     
     val sc = new SparkContext(conf) 
     
     val jssc = new StreamingContext(sc,Seconds(60))
     
     
     
     val messages = jssc.textFileStream("/user/hujw/log")
     
     messages.print()
     
     
     messages.saveAsTextFiles("/user/hujw/sparkdata/0824", "ok")
     
     
     jssc.start()
     
     jssc.awaitTermination()
     
    
  }
}