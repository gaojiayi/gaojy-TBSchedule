package com.hpe.pamirs.schedule.hpeschedule.zk;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * 与ScheduleDataManager4ZK，最好设计成继承一个抽象类 ，该类负责创建gson，zkManager等基础属性
 * @Title :zk 调度策略操作
 * @author gaojy
 *
 * @Create_Date : 2017年8月24日上午10:24:08
 * @Update_Date :
 */
public class ScheduleStrategyDataManager4ZK {

  private ZKManager zkManager;
  private String PATH_Strategy;
  private String PATH_ManagerFactory;
  private Gson gson;
  
  //在spring对象创建完毕后，创建内部对象
  public ScheduleStrategyDataManager4ZK(ZKManager aZKManager) throws Exception{
    this.zkManager = aZKManager;
    gson = new GsonBuilder()
      .registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter())
      .setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    this.PATH_Strategy = this.zkManager.getRootPath() + "/strategy";
    this.PATH_ManagerFactory = this.zkManager.getRootPath() + "/factory";
    
    if(this.getZooKeeper().exists(this.PATH_Strategy, false) == null){
      ZKTools.createPath(getZooKeeper(), this.PATH_Strategy, CreateMode.PERSISTENT, this.zkManager.getAcl());
    }
    
    if(this.getZooKeeper().exists(this.PATH_ManagerFactory, false) == null){
     ZKTools.createPath(getZooKeeper(), PATH_ManagerFactory, CreateMode.PERSISTENT, this.zkManager.getAcl());
    }
  }
  
  
  public ZooKeeper getZooKeeper() throws Exception {
    return this.zkManager.getZookeeper();
}
public String getRootPath(){
    return this.zkManager.getRootPath();
}
}


