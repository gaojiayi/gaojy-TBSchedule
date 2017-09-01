package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDeal;

/**
 * 任务调度器,在TBScheduleManager的管理下实现多线程数据处理
 * @Title :
 * @author gaojy
 *
 * @Create_Date : 2017年8月31日下午2:25:35
 * @Update_Date :
 */
public class TBScheduleProcessorSleep<T> implements IScheduleProcessor,Runnable {

  private static transient Log logger = LogFactory.getLog(TBScheduleProcessorSleep.class);
  
  final LockObject m_lockObject = new LockObject();
  
  List<Thread> threadList = new CopyOnWriteArrayList<Thread>();
  
  /**
   * 任务管理器
   */
  protected TBScheduleManager scheduleManager;
  
  /**
   * 任务类型
   */
  ScheduleTaskType taskTypeInfo;
  /**
   * 任务处理的接口类
   */
  protected IScheduleTaskDeal<T> taskDealBean;
 
  /**
   * 当前任务队列的版本号
   */
  protected long taskListVersion = 0;
  
  final Object lockVersionObject = new Object();
  final Object lockRunningList = new Object();
  
  protected List<T> taskList = new CopyOnWriteArrayList<T>();
  
  /**
   * 是否可以批处理
   */
  boolean isMutilTask = false;
  
  /**
   * 是否已经获得终止调度信号
   */
  boolean isStopSchedule = false;
  
  boolean isSleep = false;
  
  StatisticsInfo statisticsInfo;
  public void run() {
    
  }

  public boolean isDealFinishAllData() {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean isSleeping() {
    // TODO Auto-generated method stub
    return false;
  }

  public void stopSchedule() throws Exception {
    // TODO Auto-generated method stub
    
  }

  public void clearAllHasFetchData() {
    // TODO Auto-generated method stub
    
  }

}
