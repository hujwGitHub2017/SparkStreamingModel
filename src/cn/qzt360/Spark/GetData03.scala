package cn.qzt360.Spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path
import scala.util.Random

object GetData03 {
  
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
    	val info:ArrayBuffer[(String,String)] = new ArrayBuffer[(String,String)]
    	val round=new Random
			val tempk=round.nextInt(50000)
      while (x.hasNext) {
        val lineInfo = x.next()
        val infos = (lineInfo+"\tend").split("\t")
        val strImsi = infos(1).toString()
        val strImei = infos(2).toString()
        val strId = infos(0).toString()
        try {
          
          if (strId.length()>3 && strImsi.length()>5) {
        		val OperatorsId = strId.substring(strId.length()-2, strId.length())
        				val cityInfo = GetCityCodeService.getLocationByImsi(strImsi, codeMap)
        				val cityCode = cityInfo.split(",")(1)
        				info+=((OperatorsId+"_"+tempk,cityCode.substring(0, 2)+":"+strImsi+","+strImei+","+cityInfo+"\t"+strId))
        	}
          
        } catch {
          case t: Exception => t.printStackTrace() // TODO: handle error
        }
      }
      info.iterator
      
    }}
   
    val imsiRdd3 = insiRdd2.repartition(8);
    imsiRdd3.map(f=>f._2).saveAsTextFile("/user/hujw/imsinew")
    val ImsiRdd_65 = imsiRdd3.filter { x => x._2.startsWith("65") }
    ImsiRdd_65.map(f=>f._2).saveAsTextFile("/user/hujw/65new")
    val ImsiRdd_54 = imsiRdd3.filter { x => x._2.startsWith("54") }
    ImsiRdd_54.map(f=>f._2).saveAsTextFile("/user/hujw/54new")
    sc.stop()
    
  }
  
  def GetHdfsFile(fs:FileSystem,hdfsPath:String):ArrayBuffer[String]={
    val files = new ArrayBuffer[String]
    val hdfsFile = fs.listStatus(new Path(hdfsPath))
    for(f <- hdfsFile){
      
      if (f.getPath.getName.startsWith("jingxin") && !f.getPath.getName.startsWith("jingxin_201610")) {
        files.+=(f.getPath.toString())
      }
      
    }   
    files
  }
}