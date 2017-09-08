package com.hpe.pamirs.schedule.hpeschedule.strategy;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.hpe.pamirs.schedule.hpeschedule.ConsoleManager;
import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDeal;
import com.hpe.pamirs.schedule.hpeschedule.ScheduleUtil;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.IScheduleDataManager;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.TBScheduleManagerStatic;
import com.hpe.pamirs.schedule.hpeschedule.zk.ScheduleDataManager4ZK;
import com.hpe.pamirs.schedule.hpeschedule.zk.ScheduleStrategyDataManager4ZK;
import com.hpe.pamirs.schedule.hpeschedule.zk.ZKManager;

/**
 * Title: 调度服务器构造器 使用springcontext来初始化配置
 * 
 * @author gaojy
 *
 *         Create Date :2017年8月21日 Update Date :
 */
public class TBScheduleManagerFactory implements ApplicationContextAware {

  private static transient Log logger = LogFactory.getLog(TBScheduleManagerFactory.class);
  private Map<String, String> zkConfig;
  protected ZKManager zkManager;

  /**
   * 是否启动调度管理，如果只是做系统管理，应该设置为false
   */
  public boolean start = true;

  private int timerInterval = 2000;

  public volatile long timerTaskHeartBeatTS = System.currentTimeMillis();

  /**
   * 调度配置中心客户端
   */
  private IScheduleDataManager scheduleDataManager;
  private ScheduleStrategyDataManager4ZK scheduleStrategyManager;

  private Map<String, List<IStrategyTask>> managerMap =
      new ConcurrentHashMap<String, List<IStrategyTask>>();

  private ApplicationContext applicationcontext;
  private String uuid;
  private String ip;
  private String hostName;

  private Timer timer;

  /**
   * ManagerFactoryTimerTask上次执行的时间戳</br> zk环境不稳定，可能导致所有task自循环丢失，调度停止</br>
   * 外层应用，通过jmx暴露心跳时间，监控这个tbschedule最重要的大循环</br>
   */
  private ManagerFactoryTimerTask timerTask;

  protected Lock lock = new ReentrantLock();

  volatile String errorMesage = "No config Zookeeper connect infomation";
  private InitialThread initialThread;

  public TBScheduleManagerFactory() {
    this.ip = ScheduleUtil.getLocalIP();
    this.hostName = ScheduleUtil.getLocalHostName();
  }

  public void init() throws Exception {
    Properties p = new Properties();
    for (Map.Entry<String, String> e : this.zkConfig.entrySet()) {
      p.put(e.getKey(), e.getValue());
    }
    this.init(p);
  }

  public void reInit(Properties p) throws Exception {
    if (this.start == true || this.timer != null || this.managerMap.size() > 0) {
      throw new Exception("d调度器有任务处理，不能重新初始化");
    }
    this.init(p);
  }


  /**
   * 初始化 实现构造器的功能 实例化数据
   * 
   * @param p
   * @throws Exception
   */
  public void init(Properties p) throws Exception {
    // 先停止初始化线程
    if (this.initialThread != null) {
      this.initialThread.stopThread();
    }
    this.lock.lock();
    try {
      this.scheduleDataManager = null;
      this.scheduleStrategyManager = null;
      ConsoleManager.setScheduleManagerFactory(this);
      if (this.zkManager != null) {
        this.zkManager.close();
      }
      this.zkManager = new ZKManager(p);
      this.errorMesage = "Zookeeper connecting ......" + this.zkManager.getConnectStr();
      initialThread = new InitialThread(this);
      initialThread.setName("TBScheduleManagerFactory-initialThread");
      initialThread.start();
    } finally {
      this.lock.unlock();
    }
  }

  /**
   * 在zk状态正常后回调数据初始化
   * 
   * @throws Exception
   */
  public void initialData() throws Exception {
    // 创建根目录 同时设置更目录数据版本
    this.zkManager.initial();
    this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
    this.scheduleStrategyManager = new ScheduleStrategyDataManager4ZK(this.zkManager);
    if (this.start) {
      // 注册调度服务器
      this.scheduleStrategyManager.registerManagerFactory(this);


      if (timer == null) {
        timer = new Timer("TBScheduleManagerFactory-Timer");
      }

      if (timerTask == null) {
        timerTask = new ManagerFactoryTimerTask(this);
        timer.schedule(timerTask, 2000, this.timerInterval);
      }
    }
  }


  public IStrategyTask createStrategyTask(ScheduleStrategy strategy) throws Exception {
    IStrategyTask result = null;
    try {
      if (ScheduleStrategy.Kind.Schedule == strategy.getKind()) {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(strategy.getTaskName());
        String ownSign = ScheduleUtil.splitOwnsignFromTaskType(strategy.getTaskName());
        result = new TBScheduleManagerStatic(this, baseTaskType, ownSign, scheduleDataManager);
      }else if(ScheduleStrategy.Kind.Java == strategy.getKind()){
        result = (IStrategyTask)Class.forName(strategy.getTaskName()).newInstance();
        result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
      }else if(ScheduleStrategy.Kind.Bean == strategy.getKind()){
        result = (IStrategyTask)this.getBean(strategy.getTaskName());
        result.initialTaskParameter(strategy.getStrategyName(), 
                         strategy.getTaskParameter());
      }
    } catch (Exception e) {
      logger.error("strategy 获取对应的java or bean 出错,schedule并没有加载该任务,请确认" +strategy.getStrategyName(),e);
    }
    return result;
  }

  public void refresh() throws Exception{
    this.lock.lock();
    try {
      //判断状态是否终止
      ManagerFactoryInfo stsInfo = null;
      boolean isException = false;
      try {
        stsInfo = this.getScheduleStrategyManager().loadManagerFactoryInfo(this.getUuid());
      } catch (Exception e) {
        isException = true;
        logger.error("获取服务器信息有误：uuid=" + this.getUuid(),e);
      }
      
      if(isException == true){
        
      }else if(){
        
      }else{
        
      }
      
    } finally{
      // TODO: handle exception
    }
  }
  
  public void reRegisterManagerFactory() throws Exception{
    //重新分配调度器
    List<String> stopList = this.getScheduleStrategyManager().registerManagerFactory(this);
    for(String strategyName : stopList){
      this.stopServer(strategyName);
    }
    this.assignScheduleServer();
    
  }
  
  /**
   * 根据策略重新分配调度任务的机器
   * @throws Exception
   */
  public void assignScheduleServer() throws Exception{
    
  }
  
  public boolean isLeader(String uuid,List<ScheduleStrategyRunntime> factoryList){
    return start;
    
  }
  
  /**
   * 终止一类任务，如果为null，终止所有任务
   * @param strategyName
   * @throws Exception
   */
  public void stopServer(String strategyName) throws Exception{
    if(strategyName == null){
      String[] nameList = this.managerMap.keySet().toArray(new String[0]);
      for(String name : nameList){
        for(IStrategyTask task : this.managerMap.get(name)){
          try {
            task.stop(name);
          } catch (Exception e) {
            logger.error("注销任务错误：strategyName="+strategyName,e);
          }
        }
        this.managerMap.remove(name);
      }
    }else{
      List<IStrategyTask> list = this.managerMap.get(strategyName);
      if(list != null){
        for(IStrategyTask task : list){
          try {
            task.stop(strategyName);
          } catch (Exception e) {
            // TODO: handle exception
          }
        }
        this.managerMap.remove(strategyName);
      }
    }
  }
  
  
  
  /**
   * 停止所有调度资源
   * @throws Exception
   */
  public void stopAll() throws Exception{
    try {
      lock.lock();
      this.start = false;
      if(this.initialThread != null){
        this.initialThread.stopThread();
      }
      if(this.timer != null){
        if(this.timerTask != null){
          this.timerTask.cancel();
          this.timerTask = null;
        }
        this.timer.cancel();
        this.timer = null;
      }
      this.stopServer(null);
      if(this.zkManager != null){
        this.zkManager.close();
      }
      
      if(this.scheduleStrategyManager != null){
        try {
          ZooKeeper zk = this.scheduleStrategyManager.getZooKeeper();
          if(zk != null){
            zk.close();
          }
        } catch (Exception e) {
          logger.error("stopAll zk getZookeeper异常！",e);
        }
      }
      this.uuid  = null;
     logger.info("stopAll 停止服务成功！");
    } catch (Exception e) {
      logger.info("stopAll 服务失败：" + e.getMessage(),e);
    }finally{
      lock.unlock();
    }
  }
  
  
  /**
   * 重启所有的服务
   * @throws Exception
   */
  public void reStart() throws  Exception{
    try {
      if(this.timer != null){
        if(this.timerTask != null){
          this.timerTask.cancel();
          this.timerTask = null;
        }
        this.timer.purge();
      }
      this.stopServer(null);
      if(this.zkManager != null){
        this.zkManager.close();
      }
      this.uuid = null;
      this.init();
    } catch (Exception e) {
      logger.error("重启服务失败："+ e.getMessage(),e);
    }
  }
  
  public boolean isZookeeperInitialSuccess() throws Exception{
    return this.zkManager.checkZookeeperState();
  }
  
  public String[] getScheduleTaskDealList(){
    return applicationcontext.getBeanNamesForType(IScheduleTaskDeal.class);
  }
  
  public IScheduleDataManager getScheduleDataManager(){
    if(this.scheduleDataManager == null){
      throw new RuntimeException(this.errorMesage);
    }
    return scheduleDataManager;
  }
  
  public ScheduleStrategyDataManager4ZK getScheduleStrategyManager(){
    if(this.scheduleDataManager == null){
      throw new RuntimeException(this.errorMesage);
    }
    return scheduleStrategyManager; 
  }
  
  
  public void setApplicationContext(ApplicationContext aApplicationcontext) throws BeansException {
        applicationcontext = aApplicationcontext;
  }

  public ZKManager getZkManager() {
    return this.zkManager;
 }
  
  public String getUuid() {
    return uuid;
  }

  public String getIp() {
    return ip;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getHostName() {
    return hostName;
  }

  public void setStart(boolean isStart) {
    this.start = isStart;
  }

  public void setTimerInterval(int timerInterval) {
    this.timerInterval = timerInterval;
  }

  public void setZkConfig(Map<String, String> zkConfig) {
    this.zkConfig = zkConfig;
  }
  
  public Object getBean(String beanName) {
    return applicationcontext.getBean(beanName);
  }
  
  public Map<String,String> getZkConfig() {
    return zkConfig;
  }
}


/**
 * 定时调度任务
 * 
 * @author gaojy
 *
 */
class ManagerFactoryTimerTask extends TimerTask {

  private static transient Log log = LogFactory.getLog(ManagerFactoryTimerTask.class);
  TBScheduleManagerFactory factory;
  int count = 0;

  public ManagerFactoryTimerTask(TBScheduleManagerFactory aFactory) {
    this.factory = aFactory;
  }

  @Override
  public void run() {
    try {
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
      if(this.factory.zkManager.checkZookeeperState() == false){
        if(count > 5){
          log.error("Zookeeper连接失败，关闭所有的任务后，重新连接Zookeeper服务器......");
          this.factory.reStart();
        }else{
          count = count + 1;
        }
        
      }else{
        count = 0;
        this.factory.refresh();
      }
    } catch (Exception ex) {
        log.error(ex.getMessage(),ex);
    }finally{
      factory.timerTaskHeartBeatTS = System.currentTimeMillis();
    }

  }

}


class InitialThread extends Thread {
  private static transient Log log = LogFactory.getLog(InitialThread.class);
  TBScheduleManagerFactory facotry;
  boolean isStop = false;

  public InitialThread(TBScheduleManagerFactory aFactory) {
    this.facotry = aFactory;
  }

  public void stopThread() {
    this.isStop = true;
  }

  @Override
  public void run() {
    facotry.lock.lock();
    try {
      int count = 0;
      while(facotry.zkManager.checkZookeeperState() == false){
        count = count + 1;
        if(count % 50 == 0){
          facotry.errorMesage= "Zookeeper connecting ......" 
              + facotry.zkManager.getConnectStr() + " spendTime:" + count * 20 +"(ms)";
          log.error(facotry.errorMesage);
        }
        Thread.sleep(20);
        if(this.isStop)
          return ;
      }
      
      //已初始化zk后初始化数据
      facotry.initialData();
    } catch (Exception e) {
      log.error(e.getMessage(),e);
    }finally{
      facotry.lock.unlock();
    }
  }

}
