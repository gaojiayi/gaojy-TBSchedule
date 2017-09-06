package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.ThrowsAdvice;

import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDeal;
import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDealMulti;
import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDealSingle;
import com.hpe.pamirs.schedule.hpeschedule.TaskItemDefine;

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
    this.taskTypeInfo = this.scheduleManager.getTaskTypeInfo();
    this.taskDealBean = aTaskDealBean;
    if (this.taskDealBean instanceof IScheduleTaskDealSingle<?>) {
      // 单任务处理
      if (taskTypeInfo.getExecuteNumber() > 1) {
        taskTypeInfo.setExecuteNumber(1);
      }
      isMutilTask = false;
    } else {
      isMutilTask = true;
    }

    if (taskTypeInfo.getFetchDataNumber() < taskTypeInfo.getThreadNumber() * 10) {
      logger.warn("参数设置不合理，系统性能不佳。【每次从数据库获取的数量fetchnum】 >= 【线程数量threadnum】 *【最少循环次数10】 ");
    }
    for (int i = 0; i < taskTypeInfo.getThreadNumber(); i++) {
      this.startThread(i);
    }
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
    // 设置停止调度的标志，调度线程发现这个标志，执行完当前任务后，就退出调度
    this.isStopSchedule = true;
    // 清除所有未处理任务，但已经进入处理队列的，需要处理完毕
    this.taskList.clear();
  }

  private void startThread(int index) {
    Thread thread = new Thread(this);
    // 此处将调度线程保存在容器里
    threadList.add(thread);
    String threadName =
        this.scheduleManager.getScheduleServer().getTaskType() + "-"
            + this.scheduleManager.getCurrentSerialNumber() + "-exe" + index;
    thread.setName(threadName);
    thread.start();
  }

  /**
   * 判断任务是否已经处理
   * 
   * @param aTask
   * @return
   */
  protected boolean isDealing(T aTask) {
    if (this.maybeRepeatTaskList.size() == 0) {
      return false;
    }
    T[] tmpList = (T[]) this.maybeRepeatTaskList.toArray();
    for (int i = 0; i < tmpList.length; i++) {
      if (this.taskComparator.compare(aTask, tmpList[i]) == 0) {
        this.maybeRepeatTaskList.remove(tmpList[i]);
        return true;
      }
    }
    return false;
  }

  /**
   * 获取单个任务，注意lock是必须 否则在maybeRepeatTaskList的数据处理上会出现冲突
   * 
   * @return
   */
  public T getScheduleTaskId() {
    lockFetchID.lock();
    T result = null;
    try {
      while (Boolean.TRUE) {
        if (this.taskList.size() > 0) {
          result = this.taskList.remove(0);// 正序处理
        } else {
          return null;
        }

        if (this.isDealing(result) == false) {
          return result;
        }
      }
    } finally {
      lockFetchID.unlock();
    }
    return result;
  }

  /**
   * 获取多个任务，注意lock是必须的
   * 
   * @return
   */
  @SuppressWarnings("unchecked")
  public T[] getScheduleTaskIDMulti() {
    lockFetchMutilID.lock();
    try {
      if (this.taskList.size() == 0) {
        return null;
      }
      int size =
          taskList.size() > taskTypeInfo.getExecuteNumber() ? taskTypeInfo.getExecuteNumber()
              : taskList.size();

      List<T> result = new ArrayList<T>();

      int point = 0;
      T tmpObect = null;

      while (point < size && ((tmpObect = this.getScheduleTaskId()) != null)) {
        result.add(tmpObect);
        point = point + 1;
      }

      if (result.size() == 0) {
        return null;
      } else {
        return result.toArray((T[]) Array.newInstance(result.get(0).getClass(), 0));
      }
    } finally {
      lockFetchMutilID.unlock();
    }
  }


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

  /**
   * 装载数据
   * 
   * @return
   */
  protected int loadScheduleData() {
    lockLoadData.lock();
    try {
      if (this.taskList.size() > 0 || this.isStopSchedule == true) { // 判断是否有别的线程已经装载过了
        return this.taskList.size();
      }
      // 在每次数据处理完毕后休眠固定时间
      try {
        if (this.taskTypeInfo.getSleepTimeInterval() > 0) {
          if (logger.isTraceEnabled()) {
            logger.trace("处理完一批数据后休眠：" + this.taskTypeInfo.getSleepTimeInterval());
          }
          this.isSleeping = true;
          Thread.sleep(taskTypeInfo.getSleepTimeInterval());
          this.isSleeping = false;

          if (logger.isTraceEnabled()) {
            logger.trace("处理完一批数据后休眠后恢复");
          }
        }

      } catch (Exception ex) {
        logger.error("休眠时错误", ex);
      }

      putLastRunningTaskList();// 将runnning队列的数据拷贝到可能重复的队列中

      try {
        List<TaskItemDefine> taskItems = this.scheduleManager.getCurrentScheduleTaskItemList();
        // 根据队列信息查询需要调度的数据，然后增加到任务列表中
        if (taskItems.size() > 0) {
          List<TaskItemDefine> tmpTaskList = new ArrayList<TaskItemDefine>();
          synchronized (taskItems) {
            for (TaskItemDefine taskItemDefine : taskItems) {
              tmpTaskList.add(taskItemDefine);
            }
          }

          List<T> tmpList =
              this.taskDealBean.selectTask(taskTypeInfo.getTaskParameter(), scheduleManager
                  .getScheduleServer().getOwnSign(), this.scheduleManager.gettaskItemCount(),
                  tmpTaskList, taskTypeInfo.getFetchDataNumber());
          scheduleManager.getScheduleServer().setLastFetchDataTime(
              new Timestamp(scheduleManager.scheduleCenter.getSystemTime()));

          if (tmpList != null) {
            this.taskList.addAll(tmpList);
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("没有任务分配");
          }
        }

        addFetchNum(taskList.size(), "TBScheduleProcessor.loadScheduleData");

        if (taskList.size() <= 0) {
          // 判断当没有数据的时候是否需要退出调度
          if (this.scheduleManager.isContinueWhenData() == true) {
            if (taskTypeInfo.getSleepTimeNoData() > 0) {
              if (logger.isDebugEnabled()) {
                logger.debug("没有读取到需要处理 的数据，sleep" + taskTypeInfo.getSleepTimeNoData());
              }
              this.isSleeping = true;
              Thread.sleep(taskTypeInfo.getSleepTimeNoData());
              this.isSleeping = false;
            }
          }
        }
        return this.taskList.size();
      } catch (Exception ex) {
        logger.error("获取任务数据错误", ex);
      }
      return 0;
    } finally {
      lockLoadData.unlock();
    }
  }

  /**
   * 将runnning队列的数据拷贝到可能重复的队列中
   */
  public void putLastRunningTaskList() {
    lockFetchID.lock();
    try {
      this.maybeRepeatTaskList.clear();
      if (this.runningTaskList.size() == 0) {
        return;
      }
      Object[] tmpList = this.runningTaskList.toArray();
      for (int i = 0; i < tmpList.length; i++) {
        if (this.isMutilTask == false) {
          this.maybeRepeatTaskList.add((T) tmpList[i]);
        } else {
          T[] aTasks = (T[]) tmpList[i];
          for (int j = 0; j < aTasks.length; j++) {
            this.maybeRepeatTaskList.add(aTasks[j]);
          }
        }
      }
    } finally {
      lockFetchID.unlock();
    }
  }

  /**
   * 运行函数
   */
  @SuppressWarnings("unchecked")
  public void run() {
    long startTime = 0;
    long sequence = 0;
    Object executeTask = null;
    while (true) {
      try {
        if (this.isStopSchedule) { // 停止队列调度
          synchronized (this.threadList) {
            this.threadList.remove(Thread.currentThread());
            // 如果线程队列为空 则注销调度服务器
            if (this.threadList.size() == 0) {
              this.scheduleManager.unRegisterScheduleServer();
            }
          }
          return;
        }

        // 加载任务队列
        if (this.isMutilTask == false) {
          executeTask = this.getScheduleTaskId();
        } else {
          executeTask = this.getScheduleTaskIDMulti();
        }

        // 如果发现本地任务队列中没有，则加载数据
        if (executeTask == null) {
          this.loadScheduleData();
          continue;
        }


        try { // 运行相关的程序
          this.runningTaskList.add(executeTask);
          startTime = scheduleManager.scheduleCenter.getSystemTime();
          sequence = sequence + 1;
          if (this.isMutilTask == false) {
            if (((IScheduleTaskDealSingle<Object>) this.taskDealBean).execute(executeTask,
                scheduleManager.getScheduleServer().getOwnSign())) {
              addSuccessNum(1, scheduleManager.scheduleCenter.getSystemTime() - startTime,
                  "com.taobao.pamirs.schedule.TBScheduleProcessorNotSleep.run");
            } else {
              addFailNum(1, scheduleManager.scheduleCenter.getSystemTime() - startTime,
                  "com.taobao.pamirs.schedule.TBScheduleProcessorNotSleep.run");
            }
          } else { // 单任务处理
            if (((IScheduleTaskDealMulti<Object>) this.taskDealBean).execute(
                (Object[]) executeTask, scheduleManager.getScheduleServer().getOwnSign())) {

              addSuccessNum(((Object[]) executeTask).length,
                  scheduleManager.scheduleCenter.getSystemTime() - startTime,
                  "com.taobao.pamirs.schedule.TBScheduleProcessorNotSleep.run");
            } else {
              addFailNum(((Object[]) executeTask).length,
                  scheduleManager.scheduleCenter.getSystemTime() - startTime,
                  "com.taobao.pamirs.schedule.TBScheduleProcessorNotSleep.run");
            }

          }
        } catch (Throwable ex) {
          if (this.isMutilTask == false) {
            addFailNum(1, scheduleManager.scheduleCenter.getSystemTime() - startTime,
                "TBScheduleProcessor.run");
          } else {
            addFailNum(((Object[]) executeTask).length,
                scheduleManager.scheduleCenter.getSystemTime() - startTime,
                "TBScheduleProcessor.run");
          }
          logger.error("Task :" + executeTask + " 处理失败", ex);
        } finally {
          this.runningTaskList.remove(executeTask);
        }

      } catch (Exception e) {
          throw new RuntimeException(e);
      }
    }
  }

}
