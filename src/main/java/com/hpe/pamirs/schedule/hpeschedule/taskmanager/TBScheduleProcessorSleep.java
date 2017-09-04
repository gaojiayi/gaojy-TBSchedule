package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDeal;
import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDealSingle;

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
  
  boolean isSleeping = false;
  
  StatisticsInfo statisticsInfo;
  
  public TBScheduleProcessorSleep(TBScheduleManager aManager,
	IScheduleTaskDeal<T> aTaskDealBean,StatisticsInfo aStatisticsInfo ){
	  this.scheduleManager = aManager;
	  this.statisticsInfo = aStatisticsInfo;
	  this.taskTypeInfo = this.scheduleManager.getTaskTypeInfo();
	  this.taskDealBean = aTaskDealBean;
	  if(this.taskDealBean instanceof IScheduleTaskDealSingle<?>){
		  if(taskTypeInfo.getExecuteNumber() > 1){
			  taskTypeInfo.setExecuteNumber(1);
		  }
		  isMutilTask = false;
	  }else{
		  isMutilTask = true;
	  }
	  if(taskTypeInfo.getFetchDataNumber() < taskTypeInfo.getThreadNumber() * 10){
		  logger.warn("参数设置不合理，系统性能不佳。[每次从数据库获取的数量fetchnum] >= [线程数量threadnum] *[最少循环次数10]");
	  }
	  for(int i = 0;i < taskTypeInfo.getThreadNumber();i++){
		  this.st(i);
	  }
  }
  

  public void run() {
	  try {
		long startTime = 0;
		while(true){
			this.m_lockObject.addThread();
			Object executeTask;
			while(true){
				if(this.isStopSchedule == true){ //停止队列调度
					this.m_lockObject.realseThread();
					this.m_lockObject.notifyOtherThread();//通知所有的休眠线程
					synchronized (this.threadList) {
						this.threadList.remove(Thread.currentThread());
						if(this.threadList.size() == 0){
							this.scheduleManager.unRegisterScheduleServer();
						}
					}
					return;
				}
				
				//加载调度任务
				if(!this.isMutilTask){
					executeTask = this.getScheduleTaskId();
				}else{
					executeTask = this.getScheduleTaskIdMulti();
				}
				
				if(executeTask == null){
					break;
				}
				
				try {//运行相关的程序
					startTime = scheduleManager.scheduleCenter.getSystemTime();
					
					
				} catch (Exception e) {
					// TODO: handle exception
				}
				
			}
		}
	} catch (Exception e) {
		// TODO: handle exception
	}
    
  }

  public boolean isDealFinishAllData() {
    return this.taskList.size() == 0;
  }

  public boolean isSleeping() {
    return this.isSleeping;
  }

  /**
   * 需要注意的是，调用服务器从配置中心注册的工作，必须在所有线程退出的情况下才能做
   */
  public void stopSchedule() throws Exception {
    // 设置停止调度的标志，调度线程发现这个标志，执行完当前任务后，就退出调度
	  this.isStopSchedule = true;
	  //清除所有未处理任务，但已经进入处理队列，需要处理完毕
      this.taskList.clear();
  }

  public void clearAllHasFetchData() {
    this.taskList.clear();
  }
  
  private void startThread(int index){
	  Thread thread = new Thread(this);
	  threadList.add(thread);
	  String threadName = this.scheduleManager.getScheduleServer().getTaskType()
			  + "-" + this.scheduleManager.getCurrentSerialNumber() + "-exe" + index;
	  thread.setName(threadName);
	  thread.start();
  }

  public synchronized Object getScheduleTaskId(){
	  if(this.taskList.size() > 0)
		  return this.taskList.remove(0);
	  return null;
  }
  
  public synchronized Object[] getScheduleTaskIdMulti(){
	  if(this.taskList.size() == 0){
		  return null;
	  }
	  int size = taskList.size() > taskTypeInfo.getExecuteNumber()? taskTypeInfo.getExecuteNumber()
			 : taskList.size() ;
	  Object[]  result = null;
	  if(size > 0){
		  result = (Object[])Array.newInstance(this.taskList.get(0).getClass(), size);
	  }
	  for(int i = 0; i < size ;i++){
		  result[i] = this.taskList.remove(0);
	  }
	  return result;
  }
  
  protected int loadScheduleData(){
	  try {
		//在每次数据处理完毕后休眠固定的时间
		  if(this.taskTypeInfo.getSleepTimeInterval() > 0){
			  
		  }
		  
	} catch (Exception e) {
		// TODO: handle exception
	}
  }
}
