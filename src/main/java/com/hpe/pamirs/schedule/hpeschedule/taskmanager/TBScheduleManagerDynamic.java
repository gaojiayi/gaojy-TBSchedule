package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.util.List;

import com.hpe.pamirs.schedule.hpeschedule.TaskItemDefine;
import com.hpe.pamirs.schedule.hpeschedule.strategy.TBScheduleManagerFactory;
/**
 * 
 * @author gaojy
 *
 */
public class TBScheduleManagerDynamic  extends TBScheduleManager{

	public TBScheduleManagerDynamic(TBScheduleManagerFactory aFactory,
			String baseTaskType, String ownSign,
			IScheduleDataManager aScheduleCenter) throws Exception {
		super(aFactory, baseTaskType, ownSign, aScheduleCenter);
	}

	@Override
	public void initial() throws Exception {
		if(scheduleCenter
				.isLeader(this.currenScheduleServer.getUuid()
						,scheduleCenter.loadScheduleServerNames(
								this.currenScheduleServer.getTaskType()))){
		 //第一次启动，检查对于的zk目录是否存在
		this.scheduleCenter.initialRunningInfo4Dynamic(this.currenScheduleServer.getBaseTaskType(), this.currenScheduleServer.getOwnSign());
		}
		computerStart();
	}

	public void refreshScheduleServerInfo() throws Exception {
		throw new Exception("没有实现");
	}

	public boolean isNeedReLoadTaskItemList() throws Exception {
		throw new Exception("没有实现");
	}
	public void assignScheduleTask() throws Exception {
		throw new Exception("没有实现");
		
	}
	public List<TaskItemDefine> getCurrentScheduleTaskItemList() {
		throw new RuntimeException("没有实现");
	}
	public int gettaskItemCount() {
		throw new RuntimeException("没有实现");
	}


}
