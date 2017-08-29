package com.hpe.pamirs.schedule.hpeschedule.zk;


import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hpe.pamirs.schedule.hpeschedule.strategy.ScheduleStrategy;
import com.hpe.pamirs.schedule.hpeschedule.strategy.TBScheduleManagerFactory;
import com.hpe.pamirs.schedule.hpeschedule.zk.TimestampTypeAdapter;

/**
 * 与ScheduleDataManager4ZK，最好设计成继承一个抽象类 ，该类负责创建gson，zkManager等基础属性
 * 
 * @Title :zk 调度策略操作
 * @author gaojy
 *
 * @Create_Date : 2017年8月24日上午10:24:08
 * @Update_Date :
 */
public class ScheduleStrategyDataManager4ZK {

	private ZKManager zkManager;
	private String PATH_Strategy;
	private String PATH_ManagerFactory;
	private Gson gson;

	// 在spring对象创建完毕后，创建内部对象
	public ScheduleStrategyDataManager4ZK(ZKManager aZKManager)
			throws Exception {
		this.zkManager = aZKManager;
		gson = new GsonBuilder()
				.registerTypeAdapter(Timestamp.class,
						new TimestampTypeAdapter())
				.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		this.PATH_Strategy = this.zkManager.getRootPath() + "/strategy";
		this.PATH_ManagerFactory = this.zkManager.getRootPath() + "/factory";

		if (this.getZooKeeper().exists(this.PATH_Strategy, false) == null) {
			ZKTools.createPath(getZooKeeper(), this.PATH_Strategy,
					CreateMode.PERSISTENT, this.zkManager.getAcl());
		}

		if (this.getZooKeeper().exists(this.PATH_ManagerFactory, false) == null) {
			ZKTools.createPath(getZooKeeper(), PATH_ManagerFactory,
					CreateMode.PERSISTENT, this.zkManager.getAcl());
		}
	}

	public ScheduleStrategy loadStrategy(String strategyName) throws Exception{
		String zkPath = this.PATH_Strategy + "/" + strategyName;
		if(this.getZooKeeper().exists(zkPath, false) == null){
			return null;
		}
		String valueString = new String(this.getZooKeeper().getData(zkPath, false, null));
		ScheduleStrategy result = this.gson.fromJson(valueString, ScheduleStrategy.class);
		return result;
	}
	
	public void createScheduleStrategy(ScheduleStrategy scheduleStrategy)
	         throws Exception{
		String zkPath = this.PATH_Strategy + "/"
	                + scheduleStrategy.getStrategyName();
		String valueString = this.gson.toJson(scheduleStrategy);
		if(this.getZooKeeper().exists(zkPath, false) == null){
			this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
		}else{
			throw new Exception("调度策略" 
					+ scheduleStrategy.getStrategyName()
					+ "已经存在，如果确认需要重建，请先调用deleteMachineStrategy(String taskType)删除");
		}
	}
	
	public void updateScheduleStrategy(ScheduleStrategy scheduleStrategy)throws Exception{
		String zkPath = this.PATH_Strategy + "/" +scheduleStrategy.getStrategyName();
		String valueString = this.gson.toJson(scheduleStrategy);
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, valueString.getBytes(),
					this.zkManager.getAcl(), CreateMode.PERSISTENT);
		} else {
			this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
		}
	}
	
	public void deleteMachineStrategy(String taskType) throws Exception {
		deleteMachineStrategy(taskType, false);
	}
	
	public void deleteMachineStrategy(String taskType ,boolean isForce) throws Exception{
		String zkPath = this.PATH_Strategy + "/" + taskType;
		if(isForce == false
				&& this.getZooKeeper().getChildren(zkPath, null).size() > 0){
			throw new Exception("不能删除" + taskType
					+ "的运行策略，会导致必须重启整个应用才能停止失去控制的调度进程。"
					+ "可以先清空IP地址，等所有的调度器都停止后再删除调度策略");
		}
		ZKTools.deleteTree(getZooKeeper(), zkPath);
	}

	/**
	 * 加载所有的调度策略
	 * @return
	 * @throws Exception
	 */
	public List<ScheduleStrategy> loadAllScheduleStrategy() throws Exception{
		String zkPath = this.PATH_Strategy;
		List<ScheduleStrategy> result = new ArrayList<ScheduleStrategy>();
		List<String> names = this.getZooKeeper().getChildren(zkPath, false);
		Collections.sort(names);
		for(String name : names){
			result.add(this.loadStrategy(name));
		}
		return result;
		
	}
	
	public List<String> registerManagerFactory(
			TBScheduleManagerFactory managerFactory) throws Exception{
		
	}
	
	public ZooKeeper getZooKeeper() throws Exception {
		return this.zkManager.getZookeeper();
	}

	public String getRootPath() {
		return this.zkManager.getRootPath();
	}
}
