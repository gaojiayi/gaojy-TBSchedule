package com.hpe.pamirs.schedule.hpeschedule.zk;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
    if (baseTaskType.getBaseTaskType().indexOf("$") > 0) {
      throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "名称不能包括特殊字符 $");
    }

    String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType.getBaseTaskType();
    String valueString = this.gson.toJson(baseTaskType);
    if (this.getZookeeper().exists(zkPath, false) == null) {
      this.getZookeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),
          CreateMode.PERSISTENT);
    } else {
      this.getZookeeper().setData(zkPath, valueString.getBytes(), -1);
    }

  }

  public void initialRunningInfo4Dynamic(String baseTaskType, String ownSign) throws Exception {
    String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
    // 清除所有的老信息，只有leader能执行此操作
    String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType;
    if (this.getZookeeper().exists(zkPath, false) == null) {
      this.getZookeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
    }

  }


  public void initialRunningInfo4Static(String baseTaskType, String ownSign, String uuid)
      throws Exception {
    // 根据环境标志 生成一个任务类型
    String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);

    // 清除所有的老信息，只有leader能执行此操作
    String zkPath =
        this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
    try {
      ZKTools.deleteTree(this.getZookeeper(), zkPath);
    } catch (Exception e) {
      // 需要处理zookeeper session过期异常
      if (e instanceof KeeperException
          && ((KeeperException) e).code().intValue() == KeeperException.Code.SESSIONEXPIRED
              .intValue()) {
        log.warn("delete : zookeeper session已经过期，需要重新连接zookeeper");
        zkManager.reConnection();
        ZKTools.deleteTree(this.getZookeeper(), zkPath);
      }
    }
    // 创建目录
    this.getZookeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
    // 创建静态任务
    this.createScheduleTaskItem(baseTaskType, ownSign, this.loadTaskTypeBaseInfo(baseTaskType)
        .getTaskItems());
    // 标记信息初始化成功
    setInnitialRunningInfoSucuss(baseTaskType, taskType, uuid);
  }

  public void setInnitialRunningInfoSucuss(String baseTaskType, String taskType, String uuid)
      throws Exception {
    String zkPath =
        this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
    this.getZookeeper().setData(zkPath, uuid.getBytes(), -1);
  }

  public boolean isInitialRunningInfoSucuss(String baseTaskType, String ownSign) throws Exception {
    String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
    String leader = this.getLeader(this.loadScheduleServerNames(taskType));
    String zkPath =
        this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
    if (this.getZookeeper().exists(zkPath, false) != null) {
      byte[] curContent = this.getZookeeper().getData(zkPath, false, null);
      if (curContent != null && new String(curContent).equals(leader)) {
        return true;
      }
    }
    return false;
  }


  public long updateReloadTaskItemFlag(String taskType) throws Exception {
    String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
    String zkPath =
        this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
    Stat stat = this.getZookeeper().setData(zkPath, "reload=true".getBytes(), -1);
    return stat.getVersion();
  }

  
  
  public Map<String, Stat> getCurrentServerStatList(String taskType) throws Exception {
    //String :目录名     Stat: 对应的stat
    Map<String,Stat> statMap = new  HashMap<String,Stat>();
    String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
    String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType  + "/" 
              + taskType + "/" + this.PATH_Server;
    List<String> childs = this.getZookeeper().getChildren(zkPath, false);
    for(String serv : childs){
      String singleServ = zkPath  + "/" + serv;
      Stat servStat = this.getZookeeper().exists(singleServ, false);
      statMap.put(serv, servStat);
    }
    return statMap;
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

  /**
   * 加载任务类型基本信息    从/baseTaskType下获取数据
   */
  public ScheduleTaskType loadTaskTypeBaseInfo(String baseTaskType) throws Exception {
    String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType;
    if (this.getZookeeper().exists(zkPath, false) == null) {
      return null;
    }
    String valueString = new String(this.getZookeeper().getData(zkPath, false, null));
    ScheduleTaskType result =
        (ScheduleTaskType) this.gson.fromJson(valueString, ScheduleTaskType.class);
    return result;
  }


  public long getReloadTaskItemFlag(String taskType) throws Exception {
    String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
    String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType 
        + "/" + taskType + "/" + this.PATH_Server;
    Stat stat = new Stat();
    this.getZookeeper().getData(zkPath, false, stat);
    return stat.getVersion();
  }

  
  /**
   * 更新任务调度状态
   */
  public void updateScheduleTaskItemStatus(String taskType, String taskItem, TaskItemSts sts,
      String message) throws Exception {
    // TODO Auto-generated method stub

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

  /**
   * 创建任务项，注意其中的CurrentServer 和RequestDServer不会起作用
   */
  public void createScheduletaskItem(ScheduleTaskItem[] taskItems) throws Exception {
   for(ScheduleTaskItem taskItem : taskItems){
     String zkPath = this.PATH_BaseTaskType + "/" + taskItem.getBaseTaskType() 
         + "/" + taskItem.getTaskType() + this.PATH_TaskItem;
     if(this.getZookeeper().exists(zkPath, false) == null ){
       ZKTools.createPath(getZookeeper(), zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
     }
     String zkTaskItemPath = zkPath + "/" + taskItem.getTaskItem();
     this.getZookeeper().create(zkTaskItemPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
     this.getZookeeper().create(zkTaskItemPath + "/cur_server", null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
     this.getZookeeper().create(zkTaskItemPath + "/req_server", null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
     this.getZookeeper().create(zkTaskItemPath + "/sts", taskItem.getSts().toString().getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
     this.getZookeeper().create(zkTaskItemPath + "/parameter", taskItem.getDealParameter().getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
     this.getZookeeper().create(zkTaskItemPath + "/deal_desc", taskItem.getDealDesc().getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
     
   }

  }

  /**
   * 根据基础配置里面的任务项来创建各个域里面的任务项
   * @param baseTaskType
   * @param ownSign
   * @param baseTaskItems
   * @throws Exception
   */
  public void createScheduleTaskItem(String baseTaskType, String ownSign, String[] baseTaskItems)
      throws Exception {
    ScheduleTaskItem[] taskItems = new ScheduleTaskItem[baseTaskItems.length];
    // 正则表达式需要深入研究
    Pattern p = Pattern.compile("\\s*:\\s*\\{");

    for (int i = 0; i < baseTaskItems.length; i++) {
      // 初始化任务队列类型 设置任务参数
      taskItems[i] = new ScheduleTaskItem();
      taskItems[i].setBaseTaskType(baseTaskType);
      taskItems[i].setTaskType(ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign));
      taskItems[i].setOwnSign(ownSign);
      Matcher matcher = p.matcher(baseTaskItems[i]);
      if (matcher.find()) {
        taskItems[i].setTaskItem(baseTaskItems[i].substring(0, matcher.start()).trim());
        taskItems[i].setDealParameter(baseTaskItems[i].substring(matcher.end(),
            baseTaskItems[i].length() - 1).trim());
      } else {
        taskItems[i].setTaskItem(baseTaskItems[i]);
      }
      taskItems[i].setSts(ScheduleTaskItem.TaskItemSts.ACTIVTE);
    }
    createScheduletaskItem(taskItems);
  }

 

  public void deleteScheduletaskItem(String taskType, String taskItem) throws Exception {
    // TODO Auto-generated method stub

  }



  public void setInitialRunningInfoSucuss(String baseTaskType, String taskType, String uuid)
      throws Exception {
    // TODO Auto-generated method stub

  }

  public String getLeader(List<String> serverList) {
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
