package com.keyword

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.HashPartitioner

object scanfile_hdfs { 

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("flume object")
    
    conf.set("spark.speculation", "true")

    val ssc = new StreamingContext(conf, Seconds(60))
    
    //获取flume数据
   val receivepara = new ArrayBuffer[String]
    
	    receivepara+="192.168.10.14:5140"
	
	    
	     val flumestreams = receivepara.map(arg =>{
	      
	      val Array(host,port)=arg.split(":")
	      
	      FlumeUtils.createPollingStream(ssc,host, port.toString().toInt)

	      
	    })
	    
	  val filestream = ssc.union(flumestreams)

    //val filestream = ssc.textFileStream("/user/logfile/files")

    filestream.foreachRDD(rdd => {

//      val lineinfo = rdd.map(tranrdd => (new String(tranrdd)," ")).partitionBy(new HashPartitioner(50)).map(_._1).map(convert(_))
      val lineinfo = rdd.map(x => new String(x.event.getBody().array())) //.map( x => x)
      val now = Calendar.getInstance().getTimeInMillis()
     /* val dayhouseFormat = new SimpleDateFormat("yyyyMMddHH")
      val minuesecondFormat = new SimpleDateFormat("mmss")
      val ms = minuesecondFormat.format(now)
      val dh = dayhouseFormat.format(now)*/
//      lineinfo.count()
      lineinfo.foreach { x => println(x) }
      lineinfo.repartition(1).saveAsTextFile("/user/hujw/sparkdata/"+now+"/flumefile")
      rdd.unpersist(true)

    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  
}