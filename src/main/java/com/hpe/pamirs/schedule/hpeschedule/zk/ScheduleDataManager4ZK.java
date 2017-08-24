package com.hpe.pamirs.schedule.hpeschedule.zk;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.aop.ThrowsAdvice;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.hpe.pamirs.schedule.hpeschedule.ScheduleUtil;
import com.hpe.pamirs.schedule.hpeschedule.TaskItemDefine;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.IScheduleDataManager;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleServer;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleTaskItem;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleTaskItem.TaskItemSts;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleTaskType;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleTaskTypeRunningInfo;

/**
 * 
 * @Title :zk与调度任务类型数据交互类
 * @author gaojy
 *
 * @Create_Date : 2017年8月24日上午11:26:18
 * @Update_Date :
 */
public class ScheduleDataManager4ZK implements IScheduleDataManager {

  private static transient Log log = LogFactory.getLog(ScheduleDataManager4ZK.class);
  private Gson gson;
  private ZKManager zkManager;
  private String PATH_BaseTaskType;
  private String PATH_TaskItem = "taskItem";
  private String PATH_Server = "server";
  private long zkBaseTime = 0;
  private long localBaseTime = 0;

  public ScheduleDataManager4ZK(ZKManager aZKManager) throws Exception {
    this.zkManager = aZKManager;
    gson =
        new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter())
            .setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    this.PATH_BaseTaskType = this.zkManager.getRootPath() + "/baseTaskType";

    // 创建调度任务类型zookeeper节点
    if (this.getZookeeper().exists(this.PATH_BaseTaskType, false) == null) {
      ZKTools.createPath(getZookeeper(), this.PATH_BaseTaskType, CreateMode.PERSISTENT,
          this.zkManager.getAcl());
    }

    localBaseTime = System.currentTimeMillis();
    // 创建一个临时目录
    String tempPath =
        this.zkManager.getZookeeper().create(this.zkManager.getRootPath() + "/systime", null,
            this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
    Stat tempStat = this.zkManager.getZookeeper().exists(tempPath, false);
    // 得到zk当前时间
    zkBaseTime = tempStat.getCtime();
    // 删除该临时节点
    ZKTools.deleteTree(getZookeeper(), tempPath);
    // 如果zk时间
    if (Math.abs(this.zkBaseTime - this.localBaseTime) > 5000) {
      log.error("请注意，Zookeeper服务器时间与本地时间相差 ： " + Math.abs(this.zkBaseTime - this.localBaseTime)
          + " ms");
    }
  }

  public ZooKeeper getZookeeper() throws Exception {
    return this.zkManager.getZookeeper();
  }

  public void createBaseTaskType(ScheduleTaskType baseTaskType) throws Exception {
    if (baseTaskType.getBaseTaskType().indexOf("$") > 0) {
      throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "名称不能包含特殊字符$");
    }

    String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType.getBaseTaskType();
    String valueString = this.gson.toJson(baseTaskType);

    // 判断是否存在节点
    if (this.getZookeeper().exists(zkPath, false) == null) {
      this.getZookeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),
          CreateMode.PERSISTENT);
    } else {
      throw new Exception("调度任务" + baseTaskType.getBaseTaskType()
          + "已经存在,如果确认需要重建，请先调用deleteTaskType(String baseTaskType)删除");
    }
  }


  public void updateBaseTaskType(ScheduleTaskType baseTaskType) throws Exception {
   if(baseTaskType.getBaseTaskType().indexOf("$") > 0){
     throw new Exception("调度任务" + baseTaskType.getBaseTaskType() +"名称不能包括特殊字符 $");
   }

   String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType.getBaseTaskType();
   String valueString = this.gson.toJson(baseTaskType);
   if( this.getZookeeper().exists(zkPath, false) == null ){
     this.getZookeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),CreateMode.PERSISTENT);
   }else{
     this.getZookeeper().setData(zkPath, valueString.getBytes(), -1);
   }
   
  }
  
  public void initialRunningInfo4Dynamic(String baseTaskType, String ownSign) throws Exception {
   String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
   //清除所有的老信息，只有leader能执行此操作
   String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType;
   if(this.getZookeeper().exists(zkPath, false) == null){
     this.getZookeeper().create(zkPath,null, this.zkManager.getAcl(),CreateMode.PERSISTENT);
 }

  }

  
  public long getSystemTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  public List<TaskItemDefine> reloadDealTaskItem(String taskType, String uuid) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public List<ScheduleTaskItem> loadAllTaskItem(String taskType) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public void releaseDealTaskItem(String taskType, String uuid) throws Exception {
    // TODO Auto-generated method stub

  }

  public int queryTaskItemCount(String taskType) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  public ScheduleTaskType loadTaskTypeBaseInfo(String taskType) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public int clearExpireScheduleServer(String taskType, long expireTime) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  public int clearTaskItem(String taskType, List<String> serverList) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  public List<ScheduleServer> selectAllValidScheduleServer(String taskType) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> loadScheduleServerNames(String taskType) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public void assignTaskItem(String taskType, String currentUuid, int maxNumOfOneServer,
      List<String> serverList) throws Exception {
    // TODO Auto-generated method stub

  }

  public boolean refreshScheduleServer(ScheduleServer server) throws Exception {
    // TODO Auto-generated method stub
    return false;
  }

  public void registerScheduleServer(ScheduleServer server) throws Exception {
    // TODO Auto-generated method stub

  }

  public void unRegisterScheduleServer(String taskType, String serverUUID) throws Exception {
    // TODO Auto-generated method stub

  }

  public void clearExpireTaskTypeRunningInfo(String basetaskType, String serverUUID,
      double expireDateinterbal) throws Exception {
    // TODO Auto-generated method stub

  }

  public boolean isLeader(String uuid, List<String> serverList) {
    // TODO Auto-generated method stub
    return false;
  }

  public void pauseAllServer(String baseTaskType) throws Exception {
    // TODO Auto-generated method stub

  }

  public void resumeAllServer(String baseTaskType) throws Exception {
    // TODO Auto-generated method stub

  }

  public List<ScheduleTaskType> getAllTaskTypeBaseInfo() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public void clearTaskType(String baseTaskType) throws Exception {
    // TODO Auto-generated method stub

  }



  public List<ScheduleTaskTypeRunningInfo> getAllTaskTypeRunningInfo(String baseTaskType)
      throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public void deleteTaskType(String baseTaskType) throws Exception {
    // TODO Auto-generated method stub

  }

  public List<ScheduleServer> selectScheduleServer(String baseTaskType, String ownSign, String ip,
      String orderStr) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public List<ScheduleServer> selectHistoryScheduleServer(String baseTaskType, String ownSign,
      String ip, String orderStr) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public List<ScheduleServer> selectScheduleServerByManagerFactoryUUID(String factoryUUID)
      throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  public void createScheduletaskItem(ScheduleTaskItem[] taskItems) throws Exception {
    // TODO Auto-generated method stub

  }

  public void updateScheduleTaskItemStatus(String taskType, String taskItem, TaskItemSts sts,
      String message) throws Exception {
    // TODO Auto-generated method stub

  }

  public void deleteScheduletaskItem(String taskType, String taskItem) throws Exception {
    // TODO Auto-generated method stub

  }

  public void initialRunningInfo4Static(String baseTaskType, String ownSign, String uuid)
      throws Exception {
    // TODO Auto-generated method stub

  }



  public boolean isInitialRunningInfoSucuss(String baseTaskType, String ownSign) throws Exception {
    // TODO Auto-generated method stub
    return false;
  }

  public void setInitialRunningInfoSucuss(String baseTaskType, String taskType, String uuid)
      throws Exception {
    // TODO Auto-generated method stub

  }

  public String getLeader(List<String> serverList) {
    // TODO Auto-generated method stub
    return null;
  }

  public long updateReloadTaskItemFlag(String taskType) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  public long getReloadTaskItemFlag(String taskType) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  public Map<String, Stat> getCurrentServerStatList(String taskType) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

}


/**
 * 
 * @Title :日期序 列化/解序列化 实用工具类
 * @author gaojy
 * @INFO ： http://blog.csdn.net/itlwc/article/details/38454867
 * @Create_Date : 2017年8月24日上午11:02:31
 * @Update_Date :
 */
class TimestampTypeAdapter implements JsonSerializer<Timestamp>, JsonDeserializer<Timestamp> {

  public Timestamp deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    if (!(json instanceof JsonPrimitive)) {
      throw new JsonParseException("日期应该是一个String值");
    }
    try {
      DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Date data = (Date) format.parse(json.getAsString());
      return new Timestamp(data.getTime());
    } catch (Exception e) {
      throw new JsonParseException(e);
    }
  }

  public JsonElement serialize(Timestamp src, Type arg1, JsonSerializationContext arg2) {
    DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String dateFormatAsString = format.format(new Date(src.getTime()));
    return new JsonPrimitive(dateFormatAsString);
  }


}
