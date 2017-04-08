package com.database

import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import org.w3c.dom.Node
import scala.collection.mutable.Map
import scala.collection.mutable.Set
/**
 *
 *
 * @author hujw
 *
 */
object JobCmd {

  def getMonitordevice(): Option[Map[String, Any]] = {

    val conn = DBBridge.openBridge();

    val stmt = DBBridge.getStatement(conn)

    var monitordeviceMap: Option[Map[String, Any]] = None

    try { 
      var device_id:String = ""
      var strSql = "select * from tbl_monitor_device;"
      val rs = DBBridge.execSELECT(strSql, stmt, conn)
      val monitorAreaDevice:Map[String,Any] = Map()
      while(rs.next()) {
    	  val deviceInfo: Map[String, Any] = Map()
          // 字段： device_id	 设备编号  area_id	所属区域编号 device_type	设备类型 0：场所内 1: 场所外  doorway_id	 所属流通口 device_threshold 重点区域容量阈值
          device_id = rs.getString("device_id") //
          deviceInfo.put("area_id", rs.getInt("area_id"))
          deviceInfo.put("device_type", rs.getInt("device_type"))
          deviceInfo.put("doorway_id", rs.getInt("doorway_id"))
          deviceInfo.put("device_threshold", rs.getInt("device_threshold"))
          monitorAreaDevice.put(device_id, deviceInfo) // 区域作为ID，获取该区域对应的所有设备信息
      }
      if(monitorAreaDevice.size > 0){
    	  monitordeviceMap = Some(monitorAreaDevice)
      }
      DBBridge.clearResult(rs, stmt, conn)

    } catch {
      case t: Throwable => t.printStackTrace()
    }

    monitordeviceMap
  }



  def updateSchedule(CmdIdSet: Set[Int]) {

    val conn = DBBridge.openBridge()
    val stmt = DBBridge.getStatement(conn)
    try {
      for (cmdId <- CmdIdSet) {
        val strSql = "update tbl_qry_cmd set schedule = '100%' where cmd_id = '" + cmdId + "'"
        DBBridge.executeUpdate(strSql, stmt, conn)
      }
    } catch {
      case t: Throwable => {

        t.printStackTrace()

      }
    } finally {
      DBBridge.clearResult(null, stmt, conn)

    }

  }

}