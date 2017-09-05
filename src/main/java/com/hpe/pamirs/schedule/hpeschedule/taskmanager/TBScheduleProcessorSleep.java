package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDeal;
import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDealMulti;
import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDealSingle;
import com.hpe.pamirs.schedule.hpeschedule.TaskItemDefine;

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
	  //启动taskTypeInfo.getThreadNumber()个线程
	  for(int i = 0;i < taskTypeInfo.getThreadNumber();i++){
		  this.startThread(i);
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
					if(!this.isMutilTask){
					  if(((IScheduleTaskDealSingle) this.taskDealBean).execute(executeTask, scheduleManager.getScheduleServer().getOwnSign()) == true){
					     addSuccessNum(1,scheduleManager.scheduleCenter.getSystemTime()
					         - startTime,"com.taobao.pamris.schedule.TBScheduleProcessorSleep.run");
					  }else{
					    addFailNum(1,scheduleManager.scheduleCenter.getSystemTime()
					        - startTime,"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
					  }
					  
					}else{//批任务处理
					  if(((IScheduleTaskDealMulti) this.taskDealBean).execute((Object[]) executeTask,
					      scheduleManager.getScheduleServer().getOwnSign()) == true){
					    addSuccessNum(((Object[]) executeTask).length,
					        scheduleManager.scheduleCenter.getSystemTime() - startTime,
					        "com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
					  }else{
					    addFailNum(((Object[]) executeTask).length,scheduleManager.scheduleCenter.getSystemTime()
                            - startTime,
                            "com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
					  }
					}
					
				} catch (Exception ex) {
				  if (this.isMutilTask == false) {
                    addFailNum(1,scheduleManager.scheduleCenter.getSystemTime()- startTime,
                            "TBScheduleProcessor.run");
                } else {
                    addFailNum(((Object[]) executeTask).length, scheduleManager.scheduleCenter.getSystemTime()
                            - startTime,
                            "TBScheduleProcessor.run");
                }
                logger.warn("Task :" + executeTask + " 处理失败", ex);
				}
				
			}
			
			//当前队列中所有的任务都已经完成
			if(logger.isTraceEnabled()){
			  logger.trace(Thread.currentThread().getName() + ":当前运行线程数量：" + this.m_lockObject.count());
			}
			if(this.m_lockObject.realseThreadButNotLast() == false){
			  int size = 0;
			  Thread.currentThread().sleep(100);
			  startTime = scheduleManager.scheduleCenter.getSystemTime();
			  //装载数据
			  size = this.loadScheduleData();
			  if(size > 0){
			    this.m_lockObject.notifyOtherThread();
			  }else{
			    //判断当没有数据的时候，是否需要退出调度
			    if(this.isStopSchedule == false && this.scheduleManager.isContinueWhenData() == true){//继续执行
			      if(logger.isTraceEnabled()){
			          logger.trace("没有装载到数据，start sleep");
			      }
			      this.isSleeping = true;
			      Thread.currentThread().sleep(this.scheduleManager.getTaskTypeInfo().getSleepTimeNoData());
			      this.isSleeping = false;
			      
			      if(logger.isTraceEnabled()){
			        logger.trace("sleep end");
			      }
			    }else{
			      //没有数据，退出调度，唤醒所有沉睡的线程
			      this.m_lockObject.notifyOtherThread();
			    }
			  }
			  this.m_lockObject.realseThread();
			}else{
			    if(logger.isTraceEnabled()){
			      logger.trace("不是最后一个线程，sleep");
			    }
			    this.m_lockObject.waitCurrentThread();
			}
		
		}
	} catch (Exception e) {
	  logger.error(e.getMessage(), e);
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
			  if(logger.isTraceEnabled()){
			    logger.trace("处理完一批数据后休眠:" + this.taskTypeInfo.getSleepTimeInterval());
			  }
			  this.isSleeping = true;
			  Thread.sleep(taskTypeInfo.getSleepTimeInterval());
			  this.isSleeping = false;
			  
			  if(logger.isTraceEnabled()){
			    logger.trace("处理完一批数据后休眠后恢复");
			  }
		  }
	List<TaskItemDefine> taskItems  =  this.scheduleManager.getCurrentScheduleTaskItemList();
	//根据队列信息查询需要调度的数据，然后增加到任务列表中
	  if(taskItems.size() > 0 ){
	    List<TaskItemDefine> tmpTaskList = new ArrayList<TaskItemDefine>();
	    synchronized(taskItems){
	      for(TaskItemDefine taskItemDefine : taskItems){
	        tmpTaskList.add(taskItemDefine);
	      }
	    }
	    
	    List<T> tmpList = this.taskDealBean
	        .selectTask(taskTypeInfo.getTaskParameter(),
	            scheduleManager.getScheduleServer().getOwnSign(),
	            this.scheduleManager.gettaskItemCount(), tmpTaskList, 
	            taskTypeInfo.getFetchDataNumber());
	    scheduleManager.getScheduleServer().setLastFetchDataTime(new Timestamp(scheduleManager.scheduleCenter.getSystemTime()));
	  
	    if(tmpList != null){
	      this.taskList.addAll(tmpList);
	    }
	  }else{
	    if(logger.isTraceEnabled()){
          logger.trace("没有获取到需要处理的数据队列");
	      }
	  }
	  addFetchNum(taskList.size(), "TBScheduleProcessor.loadScheduleData");
	  return this.taskList.size();
	  } catch (Throwable ex) {
	    logger.error("Get tasks error.", ex);
	}
	  return 0;
  }
  
  
  public void addFetchNum(long num,String addr){
    this.statisticsInfo.addFetchDataCount(1);
    this.statisticsInfo.addFetchDataNum(num);
  }
  
  public void addSuccessNum(long num,long spendTime,String addr){
    this.statisticsInfo.addDealDataSucess(num);
    this.statisticsInfo.addDealSpendTime(spendTime);
  }
  
  public void addFailNum(long num,long spendTime,String addr){
    this.statisticsInfo.addDealDataFail(num);
    this.statisticsInfo.addDealSpendTime(spendTime);
  }
  
}
