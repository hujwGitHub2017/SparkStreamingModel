package cn.qzt360.Spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path
import scala.util.Random
import java.util.Calendar
import cn.qzt360.util.RDDMultipleTextOutputFormat

object GetData02 {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GetData")
    val sc = new SparkContext(conf)
    val hdfsConf:Configuration =  new Configuration()
    val fs = FileSystem.newInstance(hdfsConf)
    val files = GetHdfsFile(fs,"/user/wfpt/qzt")
    fs.close()
    val fileRdd = sc.textFile(files.mkString(","), 8)
    val infoRdd = fileRdd.filter { line => (line.length()>0 && (line+"\tend").split("\t").length==7)}
    
    val infoRdd2 = infoRdd.mapPartitions{ x => {
    	val info:ArrayBuffer[(String,String)] = new ArrayBuffer[(String,String)]
    	val round=new Random
			val tempk=round.nextInt(50000)
      while (x.hasNext) {
        val lineInfo = x.next()
        val infos = (lineInfo+"\tend").split("\t")
        val strId = infos(0).toString()
        if (strId.length()>3) {
        	val OperatorsId = strId.substring(strId.length()-2, strId.length())
        		info+=((OperatorsId+"_"+tempk,strId+"\t"+infos(1).toString()))
        }
      }
      info.iterator
      
    }}
   infoRdd2.saveAsHadoopFile("/user/hujw/jingxin/"+Calendar.getInstance.getTimeInMillis, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])//RDDMultipleTextOutputFormat
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