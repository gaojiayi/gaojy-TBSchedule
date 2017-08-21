package com.hpe.pamirs.schedule.hpeschedule.strategy;

public interface  IStrategyTask {

	public void initialTaskParameter(String strategyName,String taskParameter) throws Exception;
	public void stop(String strategyName) throws Exception;
}
