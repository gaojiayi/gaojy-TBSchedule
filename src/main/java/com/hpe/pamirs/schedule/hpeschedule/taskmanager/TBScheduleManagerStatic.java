package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.spi.LoggerFactory;

import com.hpe.pamirs.schedule.hpeschedule.TaskItemDefine;
import com.hpe.pamirs.schedule.hpeschedule.strategy.TBScheduleManagerFactory;

public class TBScheduleManagerStatic extends TBScheduleManager {

	private static transient Log log = LogFactory
			.getLog(TBScheduleManagerStatic.class);

	/**
	 * 总的任务量
	 */
	protected int taskItemCount = 0;

	protected long lastFetchVersion = -1;

	private final Object NeedReloadTaskItemLock = new Object();

	public TBScheduleManagerStatic(TBScheduleManagerFactory aFactory,
			String baseTaskType, String ownSign,
			IScheduleDataManager aScheduleCenter) throws Exception {
		super(aFactory, baseTaskType, ownSign, aScheduleCenter);
		// TODO Auto-generated constructor stub
	}

	public void initialRunningInfo() throws Exception {
		scheduleCenter.clearExpireScheduleServer(
				this.currenScheduleServer.getTaskType(),
				this.taskTypeInfo.getJudgeDeadInterval());
		List<String> list = scheduleCenter
				.loadScheduleServerNames(this.currenScheduleServer
						.getTaskType());
		if (scheduleCenter.isLeader(this.currenScheduleServer.getUuid(), list)) {
			// 是第一次启动，先清楚所有的垃圾数据
			log.debug(this.currenScheduleServer.getUuid() + ":" + list.size());
			this.scheduleCenter.initialRunningInfo4Static(
					this.currenScheduleServer.getBaseTaskType(),
					this.currenScheduleServer.getOwnSign(),
					this.currenScheduleServer.getUuid());
		}
	}

	@Override
	public void initial() throws Exception {
		new Thread(this.currenScheduleServer.getTaskType() + "_"
				+ this.currenScheduleServer.toString() + "-StartProcess") {
			public void run() {
				try {
					log.info("开始获取调度任务队列....of"
							+ currenScheduleServer.getUuid());

					while (!isRuntimeInfoInitial) {
						if (isStopSchedule == true) {
							log.debug("外部命令终止调度,退出调度队列获取："
									+ currenScheduleServer.getUuid());
							return;
						}

						try {
							initialRunningInfo();
							//检查是否初始化成功
							isRuntimeInfoInitial = scheduleCenter
									.isInitialRunningInfoSucuss(
											currenScheduleServer
													.getBaseTaskType(),
											currenScheduleServer.getOwnSign());
						} catch (Exception e) {
							//忽略初始化的异常
	 				    	  log.error(e.getMessage(),e);
						}
						 if(isRuntimeInfoInitial == false){
							 //还没有初始化完成 等待1秒
	    				      Thread.currentThread().sleep(1000);
						  }
					}

					int count = 0;
					lastReloadTaskItemListTime = scheduleCenter.getSystemTime();
					while(getCu){
						
					}
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		}.start();

	}

	/**
	 * 定时向数据配置中心更新当前服务器的心跳信息
	 * 如果发现本次更新的时间如果已经超过了，服务器死亡的心跳时间，则不能向服务器更新信息
	 * 而应该当做新的服务器，进行重新注册
	 */
	@Override
	public void refreshScheduleServerInfo() throws Exception {
		try {
			rewriteScheduleInfo();
			//如果任务信息没有初始化成功，不做任务相关的处理
			if(this.isRuntimeInfoInitial == false){
				return ;
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	@Override
	public void assignScheduleTask() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public List<TaskItemDefine> getCurrentScheduleTaskItemList() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int gettaskItemCount() {
		// TODO Auto-generated method stub
		return 0;
	}

}
