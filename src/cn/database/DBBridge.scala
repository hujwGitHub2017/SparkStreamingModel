package com.database

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

object DBBridge {
  
  
  val db_url = "192.168.10.20"
  
  val db_name = "beap"
  
  def openBridge():Connection = {
    
      var conn:Connection = null 
    
    try{
				Class.forName("org.postgresql.Driver").newInstance();
				 conn = DriverManager.getConnection("jdbc:postgresql://" + db_url + ":5432/" + db_name , "postgres","123456");
				if(conn.isClosed())
				{
					println("数据库未链接成功。。。");
				}
			} catch  {
			  
			  case t: Exception => {
          
          t.printStackTrace()
          
        } 
				
			}
			
			conn
			
	}
  
  def getStatement(conn:Connection):Statement={
    
    
        var stmt:Statement = null
    
          if (conn == null){
      		  
      		  println("连接还没有被建立!")
      		  
      		 }
        
        
        try
    		{
    			 stmt = conn.createStatement();
    		}catch {
    		  
    		  case e: Exception => {
    		   
              e.printStackTrace()
            } 
    	}
    
    
      stmt
  }
  
  	def execSELECT(sqls:String,stmt:Statement,conn:Connection):ResultSet={
  	  
      	  var rs:ResultSet = null
      	  
      	  
      		if (conn == null){
      		  
      		  println("连接还没有被建立!")
      		  
      		  }
      		
    		try
    		{
    			 rs = stmt.executeQuery(sqls);
    		}catch {
    		  
    		  case e: Exception => {
    		   
              e.printStackTrace()
            } 
    	}
    		
    		rs
  
  	}		
  	
  	
  	def executeUpdate(sqls:String,stmt:Statement,conn:Connection):Int = {
  	  
  	  var  numRow = 0
  	  
  	  
  	  try {
  	    
  	    if (conn == null)
  			println("连接还没有被建立!");
  		  if (sqls == null)
  			println("SQL-statement是null!");
  		
  		  conn.setAutoCommit(true);
  	  
  		if (sqls.contains("copy "))
  		{
  			stmt.executeUpdate("SET client_encoding TO 'SQL_ASCII'");
  		}
  		println("SQL: " + sqls);
  		
  		numRow = stmt.executeUpdate(sqls);
  	    
  	  } catch {
  	    case t: Throwable => t.printStackTrace() 
  	  }
  	  
  		
  		
  		numRow
  		
	}
  	
  	
  	def  clearResult(rs:ResultSet,stmt:Statement,conn:Connection){
  	  
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if(conn != null)
			  conn.close();
	}
  	
  	
  	
    
}