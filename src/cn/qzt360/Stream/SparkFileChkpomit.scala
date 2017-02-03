package cn.qzt360.Stream

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext

object SparkFileChkpomit {
  
  //构建streamingContext
  def functionToCreateContext():StreamingContext={
    
    //初始化
    val conf = new SparkConf().setAppName("FileStreaming")
    val sc = new SparkContext()
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("/user/hujwtest/checkpoint")
    val dataStream = ssc.fileStream[LongWritable,Text,TextInputFormat]("/user/hujw/data")//(c)
    //对数据进行处理
    val info = dataStream.map(lines => (lines._1))
    info.foreachRDD(rdd =>{
      rdd.foreach { x => println("info ==========="+x)}
    })
    //对元数据信息进行保存
    ssc
  }
 
 def main(args: Array[String]): Unit = {
   // 创建context
  val context = StreamingContext.getOrCreate("/user/hujwtest/checkpoint",functionToCreateContext)
  // 启动流计算
  context.start()
  context.awaitTermination()
 }
}