package com.hpe.pamirs.schedule.hpeschedule;
/**
 * 可批处理的任务类型
 * @author gaojy
 *
 * @param <T>任务类型
 */
public interface IScheduleTaskDealMulti<T> extends IScheduleTaskDeal<T> {

	/**
	 * 执行给定的任务数组。因为泛型不支持new 数组，只能传递OBJECT[]
	 * @param tasks 任务数组
	 * @param ownSign   当前环境名称
	 * @return
	 * @throws Exception
	 */
	public boolean execute(T[] tasks,String ownSign) throws Exception;
}
