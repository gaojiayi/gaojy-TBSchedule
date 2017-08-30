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
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.hpe.pamirs.schedule.hpeschedule.ScheduleUtil;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.IScheduleDataManager;
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
      }
    } catch (Exception e) {
      logger.error("");
    }
  }


  public void setApplicationContext(ApplicationContext arg0) throws BeansException {


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
    } catch (Exception e) {
      // TODO: handle exception
    }

  }

}


class InitialThread extends Thread {
  private static transient Log log = LogFactory.getLog(InitialThread.class);
  TBScheduleManagerFactory factory;
  boolean isStop = false;

  public InitialThread(TBScheduleManagerFactory aFactory) {
    this.factory = aFactory;
  }

  public void stopThread() {
    this.isStop = true;
  }

  @Override
  public void run() {
    // TODO Auto-generated method stub
    super.run();
  }

}
