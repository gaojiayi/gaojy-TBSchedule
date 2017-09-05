package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDeal;

/**
 * 任务调度器，在TBScheduleManager的管理下实现多线程数据处理
 * 
 * @Title :
 * @author gaojy
 *
 * @Create_Date : 2017年9月5日下午1:36:58
 * @Update_Date :
 */
public class TBScheduleProcessorNotSleep<T> implements IScheduleProcessor, Runnable {

  private static transient Log logger = LogFactory.getLog(TBScheduleProcessorNotSleep.class);

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
   * 任务处理接口类
   */
  protected IScheduleTaskDeal<T> taskDealBean;

  /**
   * 任务比较器
   */
  Comparator<T> taskComparator;

  /**
   * 计数器
   */
  StatisticsInfo statisticsInfo;

  /**
   * 任务列表
   */
  protected List<T> taskList = new CopyOnWriteArrayList<T>();

  /**
   * 正在处理中的任务队列
   */
  protected List<Object> runningTaskList = new CopyOnWriteArrayList<Object>();

  /**
   * 在重新取数据，可能会重复数据。 在重新取数据前，从runningTaskList拷贝得来
   */
  protected List<T> maybeRepeatTaskList = new CopyOnWriteArrayList<T>();

  Lock lockFetchID = new ReentrantLock();
  Lock lockFetchMutilID = new ReentrantLock();
  Lock lockLoadData = new ReentrantLock();

  /**
   * 是否可以批处理
   */
  boolean isMutilTask = false;

  /**
   * 是否已经获得终止调度信号
   */
  boolean isStopSchedule = false;// 用户停止队列调度
  boolean isSleeping = false;


  public TBScheduleProcessorNotSleep(TBScheduleManager aManager,
      IScheduleTaskDeal<T> aTaskDealBean, StatisticsInfo aStatisticsInfo) throws Exception {
    this.scheduleManager = aManager;
    this.statisticsInfo = aStatisticsInfo;
    this.taskComparator = new MYComparator(this.taskDealBean.getComparator());
  }



  public void run() {
    // TODO Auto-generated method stub

  }

  public boolean isDealFinishAllData() {
    return this.taskList.size() == 0 && this.runningTaskList.size() == 0;
  }

  public boolean isSleeping() {
    return this.isSleeping;
  }

  /**
   * 需要注意的是，调用服务器从配置中心注销的工作，必须在所有线程退出的情况下才能做
   */
  public void stopSchedule() throws Exception {
    //设置停止调度的标志，调度线程发现这个标志，执行完当前任务后，就退出调度
    this.isStopSchedule = true;
    //清除所有未处理任务，但已经进入处理队列的，需要处理完毕
    this.taskList.clear();
  }

  private void startThread(int index){
    Thread thread = new Thread(this);
    threadList.add(thread);
    String threadName = this.scheduleManager.getScheduleServer().getTaskType()
        + "-" + this.scheduleManager.getCurrentSerialNumber() + "-exe"
        + index;
    thread.setName(threadName);
    thread.start();
  }
  
  /**
   * 判断任务是否已经处理
   * @param aTask
   * @return
   */
  protected boolean isDealing(T aTask){
    if(this.maybeRepeatTaskList.size() == 0){
      return false;
    }
    T[] tmpList = (T[])this.maybeRepeatTaskList.toArray();
    for(int i = 0 ; i < tmpList.length ; i++){
      if(this.taskComparator.compare(aTask, tmpList[i]) == 0){
        this.maybeRepeatTaskList.remove(tmpList[i]);
        return true;
      }
    }
    return false;
  }
  
  /**
   * 获取单个任务，注意lock是必须
   * 否则在maybeRepeatTaskList的数据处理上会出现冲突
   * @return
   */
  public T getScheduleTaskId(){
    lockFetchID.lock();
    T result = null;
    try {
      while(Boolean.TRUE){
        if(this.taskList.size() > 0){
          result = this.taskList.remove(0);//正序处理
        }else{
          return null;
        }
        
        if(this.isDealing(result) == false){
          return result;
        }
      }
    } finally {
      lockFetchID.unlock();
    }
    return result;
  }
  
  public 
  
  
  public void clearAllHasFetchData() {
    this.taskList.clear();
  }


  public void addFetchNum(long num, String addr) {
    this.statisticsInfo.addFetchDataCount(1);
    this.statisticsInfo.addFetchDataNum(num);
  }

  public void addSuccessNum(long num, long spendTime, String addr) {
    this.statisticsInfo.addDealDataSucess(num);
    this.statisticsInfo.addDealSpendTime(spendTime);
  }

  public void addFailNum(long num, long spendTime, String addr) {
    this.statisticsInfo.addDealDataFail(num);
    this.statisticsInfo.addDealSpendTime(spendTime);
  }

  class MYComparator implements Comparator<T> {

    Comparator<T> comparator;

    public MYComparator(Comparator<T> aComparator) {
      this.comparator = aComparator;
    }

    public int compare(T o1, T o2) {
      statisticsInfo.addOtherCompareCount(1);
      return this.comparator.compare(o1, o2);
    }

    public boolean equals(Object obj) {
      return this.comparator.equals(obj);
    }

  }

}
