package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.text.ParseException;
import java.util.Date;
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
import org.springframework.dao.DataAccessException;

import com.hpe.pamirs.schedule.hpeschedule.CronExpression;
import com.hpe.pamirs.schedule.hpeschedule.IScheduleTaskDeal;
import com.hpe.pamirs.schedule.hpeschedule.ScheduleUtil;
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
 * @Describe 1、任务调度分配器的目标： 让所有的任务不重复，不遗漏的被快速处理。 2、一个Manager只管理一种任务类型的一组工作线程。
 *           3、在一个JVM里面可能存在多个处理相同任务类型的Manager，也可能存在处理不同任务类型的Manager。
 *           4、在不同的JVM里面可以存在处理相同任务的Manager 5、调度的Manager可以动态的随意增加和停止
 * 
 *           主要的职责： 1、定时向集中的数据配置中心更新当前调度服务器的心跳状态
 *           2、向数据配置中心获取所有服务器的状态来重新计算任务的分配。这么做的目标是避免集中任务调度中心的单点问题。
 *           3、在每个批次数据处理完毕后，检查是否有其它处理服务器申请自己把持的任务队列，如果有，则释放给相关处理服务器。
 * 
 *           其它： 如果当前服务器在处理当前任务的时候超时，需要清除当前队列，并释放已经把持的任务。并向控制主动中心报警。
 */
public abstract class TBScheduleManager implements IStrategyTask {

	private static transient Log log = LogFactory
			.getLog(TBScheduleManager.class);

	/**
	 * 用户标识不同的线程序号
	 */
	private static int nextSerialNumber = 0;

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
	protected ScheduleServer currenScheduleServer;

	/**
	 * 队列处理器
	 */
	IScheduleTaskDeal taskDealBean;

	/**
	 * 多线程任务处理器
	 */
	IScheduleProcessor processor;

	StatisticsInfo statisticsInfo = new StatisticsInfo();

	boolean isPauseSchedule = true;
	String pauseMessage = "";

	/**
	 * 当前处理任务队列清单 ArrayList实现是同步的。因多线程操作修改该列表，会造成ConcurrentModificationException
	 */
	protected List<TaskItemDefine> currentTaskItemList = new CopyOnWriteArrayList<TaskItemDefine>();

	/**
	 * 最近一起重新重新装载调度任务的时间 当前实际 - 上次装载时间 >
	 * intervalReloadTaskItemList，则向配置中心请求最新的任务分配情况
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

	public TBScheduleManager(TBScheduleManagerFactory aFactory,
			String baseTaskType, String ownSign,
			IScheduleDataManager aScheduleCenter) throws Exception {
		this.factory = aFactory;
		this.currentSerialNumber = serialNumber();
		this.scheduleCenter = aScheduleCenter;
		this.taskTypeInfo = this.scheduleCenter
				.loadTaskTypeBaseInfo(baseTaskType);
		log.info("create TBScheduleManager for taskType:" + baseTaskType);
		// 清除已经过期1天的TASK，OWN_SING的组合。超过一天没有活动的server视为过期
		this.scheduleCenter.clearExpireTaskTypeRunningInfo(baseTaskType,
				ScheduleUtil.getLocalIP() + "清除过期OWN_SIGN信息",
				this.taskTypeInfo.getExpireOwnSignInterval());

		Object dealBean = aFactory.getBean(this.taskTypeInfo.getDealBeanName());

		if (dealBean == null) {
			throw new Exception("SpringBean"
					+ this.taskTypeInfo.getDealBeanName() + "不存在");
		}
		if (dealBean instanceof IScheduleTaskDeal == false) {
			throw new Exception("springBean"
					+ this.taskTypeInfo.getDealBeanName()
					+ "没有实现IScheduleTaskDeal接口");
		}
		this.taskDealBean = (IScheduleTaskDeal) dealBean;

		// 检测配置正确性
		if (this.taskTypeInfo.getJudgeDeadInterval() < this.taskTypeInfo
				.getHeartBeatRate() * 5) {
			throw new Exception(
					"数据配置存在问题，死亡的时间间隔，至少要大于心跳频率的5倍。当前配置数据：judgeDealInterval = "
							+ this.taskTypeInfo.getJudgeDeadInterval()
							+ ",HeartbeatRate = "
							+ this.taskTypeInfo.getHeartBeatRate());
		}

		// 根据baseTaskType目录获取的数据ScheduleTaskType来创建一个ScheduleServer
		this.currenScheduleServer = ScheduleServer.createScheduleServer(
				this.scheduleCenter, baseTaskType, ownSign,
				this.taskTypeInfo.getThreadNumber());
		this.currenScheduleServer.setManagerFactoryUUID(this.factory.getUuid());
		// 注册ScheduleServer
		scheduleCenter.registerScheduleServer(this.currenScheduleServer);
		this.mBeanName = "pamire:name=" + "schedule.ServerManager."
				+ this.currenScheduleServer.getUuid();
		this.heartBeatTimer = new Timer(this.currenScheduleServer.getTaskType()
				+ "-" + this.currentSerialNumber + "-HeartBeat");
		this.heartBeatTimer.schedule(new HeartBeatTimerTask(this),
				new java.util.Date(System.currentTimeMillis() + 500),
				this.taskTypeInfo.getHeartBeatRate());
		initial();
	}

	/**
	 * 对象创建时需要做的初始化工作
	 * 
	 * @throws Exception
	 */
	public abstract void initial() throws Exception;

	public abstract void refreshScheduleServerInfo() throws Exception;

	public abstract void assignScheduleTask() throws Exception;

	public abstract List<TaskItemDefine> getCurrentScheduleTaskItemList();

	public abstract int gettaskItemCount();

	/**
	 * 清空内存中所有的已经取得的数据和任务队列， 在心跳更新失败，或者发现注册中心的调度信息被删除
	 */
	public void clearMenoInfo() {
		try {
			this.currentTaskItemList.clear();
			if (this.processor != null) {
				this.processor.clearAllHasFetchData();
			}
		} finally {
			// 设置内存里面的任务数据需要重新装载
			this.isNeedReloadtaskItem = true;
		}
	}

	public void rewriteScheduleInfo() throws Exception {
		registerLock.lock();
		try {
			if (this.isStopSchedule) {
				if (log.isDebugEnabled()) {
					log.debug("外部命令终止调度,不在注册调度服务，避免遗留垃圾数据："
							+ currenScheduleServer.getUuid());
				}
				return;
			}

			// 先发送心跳信息
			if (startErrorinfo == null) {
				this.currenScheduleServer.setDealInfoDesc(this.pauseMessage
						+ ":" + this.statisticsInfo.getDealDescription());
			} else {
				this.currenScheduleServer.setDealInfoDesc(startErrorinfo);
			}
			if (this.scheduleCenter
					.refreshScheduleServer(this.currenScheduleServer) == false) {
				// 更新信息失败，清除内存数据后重新注册
				this.clearMenoInfo();
				this.scheduleCenter
						.registerScheduleServer(this.currenScheduleServer);
			}

		} finally {
			registerLock.unlock();
		}
	}

	/**
	 * 开始的时候，计算第一次执行时间
	 * 
	 * @throws Exception
	 */
	public void computerStart() throws Exception {
		// 只有当存在可执行队列后再开始启动队列
		boolean isRunNow = false;
		if (this.taskTypeInfo.getPermitRunStartTime() == null) {
			isRunNow = true;
		} else {
			String tmpStr = this.taskTypeInfo.getPermitRunStartTime();
			if (tmpStr.toLowerCase().startsWith("startrun:")) {
				isRunNow = true;
				tmpStr = tmpStr.substring("startrun:".length());
			}
			CronExpression cexpStart = new CronExpression(tmpStr);
			Date current = new Date(this.scheduleCenter.getSystemTime());
			// 获取下次执行的开始时间
			Date firstStartTime = cexpStart.getNextInvalidTimeAfter(current);
			this.heartBeatTimer.schedule(
					new PauseOrResumeScheduleTask(this, this.heartBeatTimer
							,PauseOrResumeScheduleTask.TYPE_RESUME,tmpStr), 
					firstStartTime);
			this.currenScheduleServer.setNextRunStartTime(ScheduleUtil.transferDataToString(firstStartTime));
			
		}
	}

	public ScheduleServer getScheduleServer() {
		return this.currenScheduleServer;
	}

	public String getTaskType() {
		return this.currenScheduleServer.getTaskType();
	}

	public int getCurrentSerialNumber() {
		return this.currentSerialNumber;
	}

	private static synchronized int serialNumber() {
		return nextSerialNumber++;
	}

	public void initialTaskParameter(String strategyName, String taskParameter)
			throws Exception {
		// 没有实现的方法，需要的参数直接从任务配置中读取

	}

	/**
	 * 当服务器停止的时候，调用此方法清除所有未处理任务，清除服务器的注册信息
	 * 也可能是控制中心发起的终止指令
	 * 需要注意的是，这个方法必须在当前任务处理完毕后才能执行
	 */
	public void stop(String strategyName) throws Exception {
		if (log.isInfoEnabled()) {
			log.info("停止服务器 ：" + this.currenScheduleServer.getUuid());
		}
		this.isPauseSchedule = false;
		if (this.processor != null) {
			this.processor.stopSchedule();
		} else {
			this.unRegisterScheduleServer();
		}

	}

	/**
	 * 只应该在processor中调用
	 * 
	 * @throws Exception
	 */
	protected void unRegisterScheduleServer() throws Exception {
		registerLock.lock();
		try {
			if (this.processor != null) {
				this.processor = null;
			}

			if (this.isPauseSchedule) {
				// 是暂停调度，不注销manager自己
				return;
			}
			if (log.isDebugEnabled()) {
				log.debug("注销服务器 ：" + this.currenScheduleServer.getUuid());
			}

			this.isStopSchedule = true;
			// 取消心跳timer
			this.heartBeatTimer.cancel();
			// 从配置中心注销自己
			this.scheduleCenter.unRegisterScheduleServer(
					this.currenScheduleServer.getTaskType(),
					this.currenScheduleServer.getUuid());

		} finally {
			registerLock.unlock();
		}
	}

	/**
	 * 超过运行的运行时间，暂时停止调度
	 * 
	 * @throws Exception
	 */
	public void pause(String message) throws Exception {
		if (this.isPauseSchedule == false) {
			this.isPauseSchedule = true;
			this.pauseMessage = message;
			if (log.isDebugEnabled()) {
				log.debug("暂停调度 ：" + this.currenScheduleServer.getUuid() + ":"
						+ this.statisticsInfo.getDealDescription());
			}
			if (this.processor != null) {
				this.processor.stopSchedule();
			}
			rewriteScheduleInfo();
		}
	}

	/**
	 * 处在了可执行的时间区间，恢复运行
	 * 
	 * @param message
	 * @throws Exception
	 */
	public void resume(String message) throws Exception {
		if (this.isPauseSchedule) {
			if (log.isDebugEnabled()) {
				log.debug("恢复调度:" + this.currenScheduleServer.getUuid());
			}

			this.isPauseSchedule = false;
			this.pauseMessage = message;
			if (this.taskDealBean != null) {
				if (this.taskTypeInfo.getProcessorType() != null
						&& this.taskTypeInfo.getProcessorType()
								.equalsIgnoreCase("NOTSLEEP") == true) {
					this.taskTypeInfo.setProcessorType("NOTSLEEP");
					this.processor = new TBScheduleProcessorNotSleep(this,
							taskDealBean, this.statisticsInfo);
				} else {
					this.processor = new TBScheduleProcessorSleep(this,
							taskDealBean, this.statisticsInfo);
					this.taskTypeInfo.setProcessorType("SLEEP");
				}
			}
			rewriteScheduleInfo();
		}
	}

}

class StatisticsInfo {
	private AtomicLong fetchDataNum = new AtomicLong(0); // 读取此次数
	private AtomicLong fetchDataCount = new AtomicLong(0);// 读取的数据量
	private AtomicLong dealDataSucess = new AtomicLong(0);// 处理成功的数据量
	private AtomicLong dealDataFail = new AtomicLong(0);// 处理失败的数据量
	private AtomicLong dealSpendTime = new AtomicLong(0); // 处理总耗时，没有做同步，可能存在一定的误差
	private AtomicLong otherCompareCount = new AtomicLong(0); // 特殊比较的次数

	public void addFetchDataNum(long value) {
		this.fetchDataNum.addAndGet(value);
	}

	public void addFetchDataCount(long value) {
		this.fetchDataCount.addAndGet(value);
	}

	public void addDealDataSucess(long value) {
		this.dealDataSucess.addAndGet(value);
	}

	public void addDealDataFail(long value) {
		this.dealDataFail.addAndGet(value);
	}

	public void addDealSpendTime(long value) {
		this.dealSpendTime.addAndGet(value);
	}

	public void addOtherCompareCount(long value) {
		this.otherCompareCount.addAndGet(value);
	}

	public String getDealDescription() {
		return "FetchDataCount=" + this.fetchDataCount.get() + ",FetchDataNum="
				+ this.fetchDataNum.get() + ",DealDataSucess="
				+ this.dealDataSucess.get() + ",DealDataFail="
				+ this.dealDataFail.get() + ",DealSpendTime="
				+ this.dealSpendTime.get() + ",otherCompareCount="
				+ this.otherCompareCount.get();
	}
}

class HeartBeatTimerTask extends java.util.TimerTask {

	private static transient Log log = LogFactory
			.getLog(HeartBeatTimerTask.class);

	TBScheduleManager manager;

	public HeartBeatTimerTask(TBScheduleManager aManager) {
		manager = aManager;
	}

	@Override
	public void run() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			manager.refreshScheduleServerInfo();
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
		}

	}

}

class PauseOrResumeScheduleTask extends java.util.TimerTask {

	private static transient Log log = LogFactory
			.getLog(PauseOrResumeScheduleTask.class);

	public static int TYPE_PAUSE = 1;

	public static int TYPE_RESUME = 2;

	TBScheduleManager manager;
	Timer timer;
	int type;
	String cronTabExpress;

	public PauseOrResumeScheduleTask(TBScheduleManager aManager, Timer aTimer,
			int aType, String aCronTabExpress) {
		this.manager = aManager;
		this.timer = aTimer;
		this.type = aType;
		this.cronTabExpress = aCronTabExpress;
	}

	@Override
	public void run() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			this.cancel();// 取消调度任务
			Date current = new Date(System.currentTimeMillis());
			CronExpression cexp = new CronExpression(this.cronTabExpress);
			Date nextTime = cexp.getNextInvalidTimeAfter(current);
			if (this.type == TYPE_PAUSE) {
				manager.pause("到达终止时间,pause调度");
				this.manager.getScheduleServer().setNextRunEndTime(
						ScheduleUtil.transferDataToString(nextTime));
			} else {
				manager.resume("到达开始时间，PAUSE调度");
				this.manager.getScheduleServer().setNextRunEndTime(
						ScheduleUtil.transferDataToString(nextTime));
			}
			// 再次封装timertask，放入定时调度器执行
			this.timer.schedule(new PauseOrResumeScheduleTask(this.manager,
					this.timer, this.type, this.cronTabExpress), nextTime);
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
		}
	}

}
