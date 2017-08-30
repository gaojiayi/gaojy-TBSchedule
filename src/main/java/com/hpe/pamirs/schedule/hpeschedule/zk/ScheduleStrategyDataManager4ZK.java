package com.hpe.pamirs.schedule.hpeschedule.zk;


import java.io.Writer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hpe.pamirs.schedule.hpeschedule.strategy.ManagerFactoryInfo;
import com.hpe.pamirs.schedule.hpeschedule.strategy.ScheduleStrategy;
import com.hpe.pamirs.schedule.hpeschedule.strategy.ScheduleStrategyRunntime;
import com.hpe.pamirs.schedule.hpeschedule.strategy.TBScheduleManagerFactory;
import com.hpe.pamirs.schedule.hpeschedule.zk.TimestampTypeAdapter;

/**
 * 与ScheduleDataManager4ZK，最好设计成继承一个抽象类 ，该类负责创建gson，zkManager等基础属性
 * 
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

  // 在spring对象创建完毕后，创建内部对象
  public ScheduleStrategyDataManager4ZK(ZKManager aZKManager) throws Exception {
    this.zkManager = aZKManager;
    gson =
        new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter())
            .setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    this.PATH_Strategy = this.zkManager.getRootPath() + "/strategy";
    this.PATH_ManagerFactory = this.zkManager.getRootPath() + "/factory";

    if (this.getZooKeeper().exists(this.PATH_Strategy, false) == null) {
      ZKTools.createPath(getZooKeeper(), this.PATH_Strategy, CreateMode.PERSISTENT,
          this.zkManager.getAcl());
    }

    if (this.getZooKeeper().exists(this.PATH_ManagerFactory, false) == null) {
      ZKTools.createPath(getZooKeeper(), PATH_ManagerFactory, CreateMode.PERSISTENT,
          this.zkManager.getAcl());
    }
  }

  public ScheduleStrategy loadStrategy(String strategyName) throws Exception {
    String zkPath = this.PATH_Strategy + "/" + strategyName;
    if (this.getZooKeeper().exists(zkPath, false) == null) {
      return null;
    }
    String valueString = new String(this.getZooKeeper().getData(zkPath, false, null));
    ScheduleStrategy result = this.gson.fromJson(valueString, ScheduleStrategy.class);
    return result;
  }

  public void createScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
    String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
    String valueString = this.gson.toJson(scheduleStrategy);
    if (this.getZooKeeper().exists(zkPath, false) == null) {
      this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),
          CreateMode.PERSISTENT);
    } else {
      throw new Exception("调度策略" + scheduleStrategy.getStrategyName()
          + "已经存在，如果确认需要重建，请先调用deleteMachineStrategy(String taskType)删除");
    }
  }

  public void updateScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
    String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
    String valueString = this.gson.toJson(scheduleStrategy);
    if (this.getZooKeeper().exists(zkPath, false) == null) {
      this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),
          CreateMode.PERSISTENT);
    } else {
      this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
    }
  }

  public void deleteMachineStrategy(String taskType) throws Exception {
    deleteMachineStrategy(taskType, false);
  }

  public void deleteMachineStrategy(String taskType, boolean isForce) throws Exception {
    String zkPath = this.PATH_Strategy + "/" + taskType;
    if (isForce == false && this.getZooKeeper().getChildren(zkPath, null).size() > 0) {
      throw new Exception("不能删除" + taskType + "的运行策略，会导致必须重启整个应用才能停止失去控制的调度进程。"
          + "可以先清空IP地址，等所有的调度器都停止后再删除调度策略");
    }
    ZKTools.deleteTree(getZooKeeper(), zkPath);
  }

  /**
   * 加载所有的调度策略
   * 
   * @return
   * @throws Exception
   */
  public List<ScheduleStrategy> loadAllScheduleStrategy() throws Exception {
    String zkPath = this.PATH_Strategy;
    List<ScheduleStrategy> result = new ArrayList<ScheduleStrategy>();
    List<String> names = this.getZooKeeper().getChildren(zkPath, false);
    Collections.sort(names);
    for (String name : names) {
      result.add(this.loadStrategy(name));
    }
    return result;

  }

  /**
   * zk注册ManagerFactory
   * 
   * @param managerFactory
   * @return 需要全部注销的调度，列如当IP不在ScheduleStrategy ip列表中
   * @throws Exception
   */
  public List<String> registerManagerFactory(TBScheduleManagerFactory managerFactory)
      throws Exception {
    if (managerFactory.getUuid() == null) {
      String uuid =
          managerFactory.getIp() + "$" + managerFactory.getHostName() + "$"
              + UUID.randomUUID().toString().replaceAll("_", "").toUpperCase();
      String zkPath = this.PATH_ManagerFactory + "/" + uuid + "$";
      managerFactory.setUuid(zkPath.substring(zkPath.lastIndexOf("/") + 1));
    } else {
      String zkPath = this.PATH_ManagerFactory + "/" + managerFactory.getUuid();
      if (this.getZooKeeper().exists(zkPath, false) == null) {
        zkPath =
            this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);
      }
    }

    List<String> result = new ArrayList<String>();
    for (ScheduleStrategy schedulestrategy : loadAllScheduleStrategy()) {
      boolean isFind = false;
      // 暂时或者不在IP范围
      if (!ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(schedulestrategy.getSts())
          && schedulestrategy.getIPList() != null) {
        // 这个ip可能是是主机名 可能是个标准的IP地址
        for (String ip : schedulestrategy.getIPList()) {
          if (ip.equals("127.0.0.1") || ip.equalsIgnoreCase("localhost")
              || ip.equalsIgnoreCase(managerFactory.getHostName())
              || ip.equals(managerFactory.getIp())) {
            // 添加可管理TaskType
            String zkPath =
                this.PATH_Strategy + "/" + schedulestrategy.getStrategyName() + "/"
                    + managerFactory.getUuid();
            if (this.getZooKeeper().exists(zkPath, false) == null) {
              zkPath =
                  this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(),
                      CreateMode.EPHEMERAL);
            }

            isFind = true;
            break;
          }
        }
      }

      if (!isFind) { // 清除原来注册的Factory
        String zkPath =
            this.PATH_Strategy + "/" + schedulestrategy.getStrategyName() + "/"
                + managerFactory.getUuid();
        if (this.getZooKeeper().exists(zkPath, false) != null) {
          ZKTools.deleteTree(getZooKeeper(), zkPath);
          result.add(schedulestrategy.getStrategyName());
        }
      }
    }
    return result;
  }

  /**
   * 注销服务 停止调度 (删除strategy上对应的uuid节点)
   * 
   * @param managerFactory
   * @throws Exception
   */
  public void unRregisterManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception {
    for (String strategyName : this.getZooKeeper().getChildren(this.PATH_Strategy, false)) {
      String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + managerFactory.getUuid();
      if (this.getZooKeeper().exists(zkPath, false) != null) {
        ZKTools.deleteTree(this.getZooKeeper(), zkPath);
      }
    }
  }

  public ScheduleStrategyRunntime loadScheduleStrategyRunntime(String strategyName, String uuid)
      throws Exception {
    String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + uuid;
    ScheduleStrategyRunntime result = null;
    if (this.getZooKeeper().exists(zkPath, false) != null) {
      // 从strategy的uuid目录节点获取数据
      byte[] value = this.getZooKeeper().getData(zkPath, false, null);
      if (value != null) {
        String valueString = new String(value);
        result =
            (ScheduleStrategyRunntime) this.gson.fromJson(valueString,
                ScheduleStrategyRunntime.class);
        if (null != result) {
          throw new Exception("gson 反序列化异常，对象为null");
        }
        if (null == result.getStrategyName()) {
          throw new Exception("gson 反序列化异常，策略名字为null");
        }
        if (null == result.getUuid()) {
          throw new Exception("gson 反序列化异常，uuid为null");
        }

      } else {
        result = new ScheduleStrategyRunntime();
        result.setStrategyName(strategyName);
        result.setUuid(uuid);
        // 需要的任务数量为 0
        result.setRequestNum(0);
        result.setMessage("");
      }
    }
    return result;
  }

  /**
   * 装载所有的策略运行状态
   * 
   * @return
   * @throws Exception
   */
  public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntime() throws Exception {
    List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
    String zkPath = this.PATH_Strategy;
    for (String strategyType : this.getZooKeeper().getChildren(zkPath, false)) {
      for (String uuid : this.getZooKeeper().getChildren(zkPath + "/" + strategyType, false)) {
        result.add(loadScheduleStrategyRunntime(strategyType, uuid));
      }
    }
    return result;
  }

  /**
   * 根据uuid，获取运行时调度策略
   * 
   * @param managerFactoryUUID
   * @return
   * @throws Exception
   */
  public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByUUID(
      String managerFactoryUUID) throws Exception {
    List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
    String zkPath = this.PATH_Strategy;

    List<String> taskTypeList = this.getZooKeeper().getChildren(zkPath, false);
    Collections.sort(taskTypeList);
    for (String taskType : taskTypeList) {
      if (this.getZooKeeper().exists(zkPath + "/" + taskType + "/" + managerFactoryUUID, false) != null) {
        result.add(loadScheduleStrategyRunntime(taskType, managerFactoryUUID));
      }
    }
    return result;
  }

  public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByTaskType(
      String strategyName) throws Exception {
    List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
    String zkPath = this.PATH_Strategy;
    if (this.getZooKeeper().exists(zkPath + "/" + strategyName, false) == null) {
      return result;
    }
    List<String> uuidList = this.getZooKeeper().getChildren(zkPath + "/" + strategyName, false);

    // 排序
    Collections.sort(uuidList, new Comparator<String>() {

      public int compare(String u1, String u2) {
        return u1.substring(u1.lastIndexOf("$") + 1).compareTo(
            u2.substring(u2.lastIndexOf("$") + 1));
      }
    });

    for (String uuid : uuidList) {
      result.add(loadScheduleStrategyRunntime(strategyName, uuid));
    }
    return result;
  }

  /**
   * 更新请求数量
   * 
   * @param strategyName
   * @param managerFactoryUUID
   * @param requestNum
   * @throws Exception
   */
  public void updateStrategyRunntimeRequestNum(String strategyName, String managerFactoryUUID,
      int requestNum) throws Exception {
    String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + managerFactoryUUID;
    ScheduleStrategyRunntime result = null;
    if (this.getZooKeeper().exists(zkPath, false) != null) {
      result = this.loadScheduleStrategyRunntime(strategyName, managerFactoryUUID);
    } else {
      result = new ScheduleStrategyRunntime();
      result.setStrategyName(strategyName);
      result.setUuid(managerFactoryUUID);
      result.setMessage("");
    }
    result.setRequestNum(requestNum);
    String valueString = this.gson.toJson(result);
    this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
  }



  public void updateStrategyRunntimeErrorMessage(String strategyName, String managerFactoryUUID,
      String message) throws Exception {
    String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + managerFactoryUUID;
    ScheduleStrategyRunntime result = null;
    if (this.getZooKeeper().exists(zkPath, false) != null) {
      result = this.loadScheduleStrategyRunntime(strategyName, managerFactoryUUID);
    } else {
      result = new ScheduleStrategyRunntime();
      result.setStrategyName(strategyName);
      result.setUuid(managerFactoryUUID);
      result.setRequestNum(0);
    }
    result.setMessage(message);
    String valueString = this.gson.toJson(result);
    this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
  }

  /**
   * 更新任务管理器
   * 
   * @param uuid
   * @param isStart
   * @throws Exception
   */
  public void updateManagerFactoryInfo(String uuid, boolean isStart) throws Exception {
    String zkPath = this.PATH_ManagerFactory + "/" + uuid;
    if (this.getZooKeeper().exists(zkPath, false) == null) {
      throw new Exception("任务管理器不存在：" + uuid);
    }
    this.getZooKeeper().setData(zkPath, Boolean.toString(isStart).getBytes(), -1);
  }

  public ManagerFactoryInfo loadManagerFactoryInfo(String uuid) throws Exception {
    String zkPath = this.PATH_ManagerFactory + "/" + uuid;
    if (this.getZooKeeper().exists(zkPath, false) == null) {
      throw new Exception("任务管理器不存在：" + uuid);
    }
    byte[] value = this.getZooKeeper().getData(zkPath, false, null);
    ManagerFactoryInfo result = new ManagerFactoryInfo();
    result.setUuid(uuid);
    if (value == null) {
      result.setStart(true);
    } else {
      result.setStart(Boolean.parseBoolean(new String(value)));
    }
    return result;
  }
  
  
  /**
   * 导入配置信息【目前支持baseTaskType和strategy数据】
   * @param config
   * @param writer
   * @param isUpdate
   * @throws Exception
   */
  public void importConfig(String config , Writer writer,boolean isUpdate) 
      throws Exception{
    ConfigNode configNode = gson.fromJson(config, ConfigNode.class);
    if(configNode != null){
      String path = configNode.getRootPath() + "/" 
           + configNode.getConfigType();
      ZKTools.createPath(getZooKeeper(), path, CreateMode.PERSISTENT, zkManager.getAcl());
      String y_node = path + "/" + configNode.getName();
      if(getZooKeeper().exists(y_node, false) == null){
        writer.append("<font color=\"red\">成功导入新配置信息\n</font>");
        getZooKeeper().create(y_node, configNode.getValue().getBytes(), zkManager.getAcl(), CreateMode.PERSISTENT);
      }else if(isUpdate){
        writer.append("<font color=\"red\">该配置信息已经存在，并且强制更新了\n</font>");
        getZooKeeper().setData(y_node, configNode.getValue().getBytes(), -1);
      }else{
        writer.append("<font color=\"red\">该配置信息已经存在，如需要更新，请配置强制更新\n</font>");
      }
    }
    writer.append(configNode.toString());
  }

  public ZooKeeper getZooKeeper() throws Exception {
    return this.zkManager.getZookeeper();
  }

  public String getRootPath() {
    return this.zkManager.getRootPath();
  }
}
