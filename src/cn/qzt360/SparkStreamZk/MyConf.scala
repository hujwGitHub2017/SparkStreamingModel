package cn.qzt360.kafka

import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient

/**
 * @author Administrator
 */
class MyConf extends Serializable {
  @transient
  var topicDirs:ZKGroupTopicDirs = null//new ZKGroupTopicDirs("hyj", topic2)
  @transient
  var zkTopicPath:String = null
  @transient
  var zkClient:ZkClient = null
  
  def this(s1: String,s2:String) {
    this()
    topicDirs=new ZKGroupTopicDirs(s1, s2)
    zkTopicPath=s"${topicDirs.consumerOffsetDir}" 
    zkClient=new ZkClient("192.168.10.10:2181")
  }
}