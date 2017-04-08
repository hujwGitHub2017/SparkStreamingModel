package cn.qzt360.SparkStreamZk

import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient

class zkConf {
  @transient
  var zkTopicDirs:ZKGroupTopicDirs = null
  @transient
  var zkTopicPath:String = ""
  @transient
  var zkClient:ZkClient = null
  
  def this(groupId:String,topicId:String){
    this()
    zkTopicDirs =  new ZKGroupTopicDirs(groupId,topicId)
    zkTopicPath = s"${zkTopicDirs.consumerOffsetDir}"
    zkClient=new ZkClient("192.168.10.10:2181")
  }
}