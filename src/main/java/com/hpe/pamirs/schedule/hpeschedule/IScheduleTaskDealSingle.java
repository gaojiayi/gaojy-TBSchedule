package com.hpe.pamirs.schedule.hpeschedule;
/**
 * 单个任务处理接口
 * @author gaojy
 *
 * @param <T>任务类型
 */
public interface IScheduleTaskDealSingle<T> extends IScheduleTaskDeal<T> {

	/**
	 * 执行单个任务
	 * @param task
	 * @param ownSign
	 * @return
	 * @throws Exception
	 */
	public boolean execute(T task,String ownSign) throws Exception;
	
}
