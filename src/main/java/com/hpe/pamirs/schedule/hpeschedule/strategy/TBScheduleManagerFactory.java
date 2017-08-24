package com.hpe.pamirs.schedule.hpeschedule.strategy;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.spi.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.hpe.pamirs.schedule.hpeschedule.taskmanager.IScheduleDataManager;
import com.hpe.pamirs.schedule.hpeschedule.zk.ZKManager;
/**
 * Title: 调度服务器构造器
 * @author gaojy
 *
 * Create Date :2017年8月21日
 * Update Date :
 */
public class TBScheduleManagerFactory implements ApplicationContextAware{

	private static transient Log logger = LogFactory.getLog(TBScheduleManagerFactory.class);
	private Map<String ,String> zkConfig;
	protected ZKManager zkManager;
	
	/**
	 * 是否启动调度管理，如果只是做系统管理，应该设置为false
	 */
	public boolean start = true;
	
	private int timeInterval = 2000;
	
	/**
	 * ManagerFactoryTimerTask上次执行的时间戳</br>
	 * zk环境不稳定，可能导致所有task自循环丢失，调度停止</br>
	 * 外层应用，通过jmx暴露心跳时间，监控这个tbschedule最重要的大循环</br>
	 */
	public volatile long timerTaskHeartBeatTS = System.currentTimeMillis();
	
	/**
	 * 调度配置中心客户端
	 */
	private IScheduleDataManager scheduleDataManager;
	private ScheduleStrategyDataManager4ZK scheduleStrategyManager;
	
	public void setApplicationContext(ApplicationContext arg0) throws BeansException {
		// TODO Auto-generated method stub
		
	}

}
