package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.util.List;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.spi.LoggerFactory;
import org.springframework.aop.target.PrototypeTargetSource;

import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDeal;
import com.hpe.pamirs.schedule.hpeschedule.TaskItemDefine;
import com.hpe.pamirs.schedule.hpeschedule.strategy.IStrategyTask;
import com.hpe.pamirs.schedule.hpeschedule.strategy.TBScheduleManagerFactory;
/**
 * 
 * @Title :
 * @author gaojy
 *
 * @Create_Date : 2017年8月23日下午3:30:58
 * @Update_Date :
 * @Describe 
 *    1、任务调度分配器的目标： 让所有的任务不重复，不遗漏的被快速处理。
 *    2、一个Manager只管理一种任务类型的一组工作线程。
 *    3、在一个JVM里面可能存在多个处理相同任务类型的Manager，也可能存在处理不同任务类型的Manager。
 *    4、在不同的JVM里面可以存在处理相同任务的Manager 
 *    5、调度的Manager可以动态的随意增加和停止
 *    
 *  主要的职责：
 * 1、定时向集中的数据配置中心更新当前调度服务器的心跳状态
 * 2、向数据配置中心获取所有服务器的状态来重新计算任务的分配。这么做的目标是避免集中任务调度中心的单点问题。
 * 3、在每个批次数据处理完毕后，检查是否有其它处理服务器申请自己把持的任务队列，如果有，则释放给相关处理服务器。
 *  
 * 其它：
 *   如果当前服务器在处理当前任务的时候超时，需要清除当前队列，并释放已经把持的任务。并向控制主动中心报警。
 */
public abstract class TBScheduleManager implements IStrategyTask{
  
  private static transient Log log = LogFactory.getLog(TBScheduleManager.class);

  /**
   * 用户标识不同的线程序号
   */
	private static int  nextSerialNumber = 0;
  
  /**
   * 当前线程组编号
   */
  protected int currentSerialNumber = 0;
  
  /**
   * 调度任务类型信息
   */
  protected ScheduleTaskType taskTypeInfo;
  
  /**
   * 当前调度服务的信息
   */
  protected ScheduleServer currentScheduleServer;
  
  /**
   * 队列处理器
   */
  IScheduleTaskDeal taskDealBean;
  
  /**
   * 多线程任务处理器
   */
  IScheduleProcessor processor;
  
  StatisticsInfo  statisticsInfo = new StatisticsInfo();
  
  boolean isPauseSchedule = true;
  String pauseMessage="";
  
  /**
   * 当前处理任务队列清单
   * ArrayList实现是同步的。因多线程操作修改该列表，会造成ConcurrentModificationException
   */
  protected List<TaskItemDefine> currentTaskItemList = new CopyOnWriteArrayList<TaskItemDefine>();
  
  /**
   * 最近一起重新重新装载调度任务的时间
   * 当前实际  - 上次装载时间  > intervalReloadTaskItemList，则向配置中心请求最新的任务分配情况
   */
  protected long lastReloadTaskItemListTime = 0;
  protected boolean isNeedReloadtaskItem = true;
  
  private String mBeanName;
  
  /**
   * 向配置中心更新信息的定时器
   */
  private Timer heartBeatTimer;
  
  protected IScheduleDataManager scheduleCenter;
  
  protected String startErrorinfo = null;
  
  protected boolean isStopSchedule = false;
  
  protected Lock registerLock = new ReentrantLock();
  
  /**
   * 运行期信息是否初始化成功
   */
  protected boolean isRuntimeInfoInitial = false;
  
  TBScheduleManagerFactory factory;
  
  public TBScheduleManager(TBScheduleManagerFactory aFactory,String baseTaskType,String ownSign,IScheduleDataManager aScheduleCenter) {
	this.factory = aFactory;
	this.currentSerialNumber = serialNumber();
	this.scheduleCenter = aScheduleCenter;
	this.taskTypeInfo =  this.scheduleCenter.load
}
  
  
  private static synchronized int serialNumber() {
      return nextSerialNumber++;
}		
  
  public void initialTaskParameter(String strategyName, String taskParameter) throws Exception {
    // TODO Auto-generated method stub
    
  }

  public void stop(String strategyName) throws Exception {
    // TODO Auto-generated method stub
    
  }
  
  class StatisticsInfo{
    private AtomicLong fetchDataNum = new AtomicLong(0); //读取此次数
    private AtomicLong fetchDataCount = new AtomicLong(0);//读取的数据量
    private AtomicLong dealDataSucess = new AtomicLong(0);//处理成功的数据量
    private AtomicLong dealDataFail = new AtomicLong(0);//处理失败的数据量
    private AtomicLong dealSpendTime = new AtomicLong(0); //处理总耗时，没有做同步，可能存在一定的误差
    private AtomicLong otherCompareCount = new AtomicLong(0); //特殊比较的次数
    
    
    public void addFetchDataNum(long value){
      this.fetchDataNum.addAndGet(value);
  }
  public void addFetchDataCount(long value){
      this.fetchDataCount.addAndGet(value);
  }
  public void addDealDataSucess(long value){
      this.dealDataSucess.addAndGet(value);
  }
  public void addDealDataFail(long value){
      this.dealDataFail.addAndGet(value);
  }
  public void addDealSpendTime(long value){
      this.dealSpendTime.addAndGet(value);
  }
  public void addOtherCompareCount(long value){
      this.otherCompareCount.addAndGet(value);
  }
  public String getDealDescription(){
      return "FetchDataCount=" + this.fetchDataCount.get()
        +",FetchDataNum=" + this.fetchDataNum.get()
        +",DealDataSucess=" + this.dealDataSucess.get()
        +",DealDataFail=" + this.dealDataFail.get()
        +",DealSpendTime=" + this.dealSpendTime.get()
        +",otherCompareCount=" + this.otherCompareCount.get();          
  }
  }

}
