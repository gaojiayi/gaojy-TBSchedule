package com.hpe.pamirs.schedule.hpeschedule.zk;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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
 * 
 * @Title :
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
    gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter())
  }
}


