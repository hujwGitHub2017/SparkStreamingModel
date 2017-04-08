package cn.qzt360.Spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path

object GetData {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GetData")
    val sc = new SparkContext(conf)
    val hdfsConf:Configuration =  new Configuration()
    val fs = FileSystem.newInstance(hdfsConf)
    val files = GetHdfsFile(fs,"/user/wfpt/qzt")
    fs.close()
    //GetCityCodeService.initImsiLocation()
    val fileRdd = sc.textFile(files.mkString(","), 8)
    val imsiRdd = fileRdd.filter { line => (line.length()>0 && (line+"\tend").split("\t").length==7)}
    val insiRdd2 = imsiRdd.mapPartitions{ x => {
    	val codeMap = GetCityCodeService.initImsiLocation()
    	val info:ArrayBuffer[String] = new ArrayBuffer[String]
      while (x.hasNext) {
        val lineInfo = x.next()
        val strImsi = lineInfo.split("\t")(1).toString()
        val cityInfo = GetCityCodeService.getLocationByImsi(strImsi, codeMap)
        val cityCode = cityInfo.split(",")(1)
        info+=cityCode.substring(0, 2)+":"+strImsi+","+cityInfo
      }
      info.iterator
      
    }}
   
    val imsiRdd3 = insiRdd2.repartition(8);
    imsiRdd3.saveAsTextFile("/user/hujw/imsi")
    val ImsiRdd_65 = imsiRdd3.filter { x => x.startsWith("65") }
    ImsiRdd_65.saveAsTextFile("/user/hujw/65")
    val ImsiRdd_54 = imsiRdd3.filter { x => x.startsWith("54") }
    ImsiRdd_54.saveAsTextFile("/user/hujw/54")
    sc.stop()
    
  }
  
  def GetHdfsFile(fs:FileSystem,hdfsPath:String):ArrayBuffer[String]={
    val files = new ArrayBuffer[String]
    val hdfsFile = fs.listStatus(new Path(hdfsPath))
    for(f <- hdfsFile){
      
      if (f.getPath.getName.startsWith("jingxin")) {
        files.+=(f.getPath.toString())
      }
      
    }   
    files
  }
}