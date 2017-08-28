package com.hpe.pamirs.schedule.hpeschedule.zk;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.aop.ThrowsAdvice;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.hpe.pamirs.schedule.hpeschedule.ScheduleUtil;
import com.hpe.pamirs.schedule.hpeschedule.TaskItemDefine;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.IScheduleDataManager;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleServer;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleTaskItem;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleTaskItem.TaskItemSts;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleTaskType;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleTaskTypeRunningInfo;

/**
 * 
 * @Title :zk与调度任务类型数据交互类
 * @author gaojy
 *
 * @Create_Date : 2017年8月24日上午11:26:18
 * @Update_Date :
 */
public class ScheduleDataManager4ZK implements IScheduleDataManager {

	private static transient Log log = LogFactory
			.getLog(ScheduleDataManager4ZK.class);
	private Gson gson;
	private ZKManager zkManager;
	private String PATH_BaseTaskType;
	private String PATH_TaskItem = "taskItem";
	private String PATH_Server = "server";
	private long zkBaseTime = 0;
	private long localBaseTime = 0;

	public ScheduleDataManager4ZK(ZKManager aZKManager) throws Exception {
		this.zkManager = aZKManager;
		gson = new GsonBuilder()
				.registerTypeAdapter(Timestamp.class,
						new TimestampTypeAdapter())
				.setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		this.PATH_BaseTaskType = this.zkManager.getRootPath() + "/baseTaskType";

		// 创建调度任务类型zookeeper节点
		if (this.getZookeeper().exists(this.PATH_BaseTaskType, false) == null) {
			ZKTools.createPath(getZookeeper(), this.PATH_BaseTaskType,
					CreateMode.PERSISTENT, this.zkManager.getAcl());
		}

		localBaseTime = System.currentTimeMillis();
		// 创建一个临时目录
		String tempPath = this.zkManager.getZookeeper().create(
				this.zkManager.getRootPath() + "/systime", null,
				this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
		Stat tempStat = this.zkManager.getZookeeper().exists(tempPath, false);
		// 得到zk当前时间
		zkBaseTime = tempStat.getCtime();
		// 删除该临时节点
		ZKTools.deleteTree(getZookeeper(), tempPath);
		// 如果zk时间
		if (Math.abs(this.zkBaseTime - this.localBaseTime) > 5000) {
			log.error("请注意，Zookeeper服务器时间与本地时间相差 ： "
					+ Math.abs(this.zkBaseTime - this.localBaseTime) + " ms");
		}
	}

	public ZooKeeper getZookeeper() throws Exception {
		return this.zkManager.getZookeeper();
	}

	public void createBaseTaskType(ScheduleTaskType baseTaskType)
			throws Exception {
		if (baseTaskType.getBaseTaskType().indexOf("$") > 0) {
			throw new Exception("调度任务" + baseTaskType.getBaseTaskType()
					+ "名称不能包含特殊字符$");
		}

		String zkPath = this.PATH_BaseTaskType + "/"
				+ baseTaskType.getBaseTaskType();
		String valueString = this.gson.toJson(baseTaskType);

		// 判断是否存在节点
		if (this.getZookeeper().exists(zkPath, false) == null) {
			this.getZookeeper().create(zkPath, valueString.getBytes(),
					this.zkManager.getAcl(), CreateMode.PERSISTENT);
		} else {
			throw new Exception("调度任务" + baseTaskType.getBaseTaskType()
					+ "已经存在,如果确认需要重建，请先调用deleteTaskType(String baseTaskType)删除");
		}
	}

	public void updateBaseTaskType(ScheduleTaskType baseTaskType)
			throws Exception {
		if (baseTaskType.getBaseTaskType().indexOf("$") > 0) {
			throw new Exception("调度任务" + baseTaskType.getBaseTaskType()
					+ "名称不能包括特殊字符 $");
		}

		String zkPath = this.PATH_BaseTaskType + "/"
				+ baseTaskType.getBaseTaskType();
		String valueString = this.gson.toJson(baseTaskType);
		if (this.getZookeeper().exists(zkPath, false) == null) {
			this.getZookeeper().create(zkPath, valueString.getBytes(),
					this.zkManager.getAcl(), CreateMode.PERSISTENT);
		} else {
			this.getZookeeper().setData(zkPath, valueString.getBytes(), -1);
		}

	}

	public void initialRunningInfo4Dynamic(String baseTaskType, String ownSign)
			throws Exception {
		String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(
				baseTaskType, ownSign);
		// 清除所有的老信息，只有leader能执行此操作
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType;
		if (this.getZookeeper().exists(zkPath, false) == null) {
			this.getZookeeper().create(zkPath, null, this.zkManager.getAcl(),
					CreateMode.PERSISTENT);
		}

	}

	public void initialRunningInfo4Static(String baseTaskType, String ownSign,
			String uuid) throws Exception {
		// 根据环境标志 生成一个任务类型
		String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(
				baseTaskType, ownSign);

		// 清除所有的老信息，只有leader能执行此操作
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_TaskItem;
		try {
			ZKTools.deleteTree(this.getZookeeper(), zkPath);
		} catch (Exception e) {
			// 需要处理zookeeper session过期异常
			if (e instanceof KeeperException
					&& ((KeeperException) e).code().intValue() == KeeperException.Code.SESSIONEXPIRED
							.intValue()) {
				log.warn("delete : zookeeper session已经过期，需要重新连接zookeeper");
				zkManager.reConnection();
				ZKTools.deleteTree(this.getZookeeper(), zkPath);
			}
		}
		// 创建目录
		this.getZookeeper().create(zkPath, null, this.zkManager.getAcl(),
				CreateMode.PERSISTENT);
		// 创建静态任务
		this.createScheduleTaskItem(baseTaskType, ownSign, this
				.loadTaskTypeBaseInfo(baseTaskType).getTaskItems());
		// 标记信息初始化成功
		setInnitialRunningInfoSucuss(baseTaskType, taskType, uuid);
	}

	public void setInnitialRunningInfoSucuss(String baseTaskType,
			String taskType, String uuid) throws Exception {
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_TaskItem;
		this.getZookeeper().setData(zkPath, uuid.getBytes(), -1);
	}

	public boolean isInitialRunningInfoSucuss(String baseTaskType,
			String ownSign) throws Exception {
		String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(
				baseTaskType, ownSign);
		String leader = this.getLeader(this.loadScheduleServerNames(taskType));
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_TaskItem;
		if (this.getZookeeper().exists(zkPath, false) != null) {
			byte[] curContent = this.getZookeeper()
					.getData(zkPath, false, null);
			if (curContent != null && new String(curContent).equals(leader)) {
				return true;
			}
		}
		return false;
	}

	public long updateReloadTaskItemFlag(String taskType) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_Server;
		Stat stat = this.getZookeeper().setData(zkPath,
				"reload=true".getBytes(), -1);
		return stat.getVersion();
	}

	public Map<String, Stat> getCurrentServerStatList(String taskType)
			throws Exception {
		// String :目录名 Stat: 对应的stat
		Map<String, Stat> statMap = new HashMap<String, Stat>();
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_Server;
		List<String> childs = this.getZookeeper().getChildren(zkPath, false);
		for (String serv : childs) {
			String singleServ = zkPath + "/" + serv;
			Stat servStat = this.getZookeeper().exists(singleServ, false);
			statMap.put(serv, servStat);
		}
		return statMap;
	}

	public long getReloadTaskItemFlag(String taskType) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_Server;
		Stat stat = new Stat();
		this.getZookeeper().getData(zkPath, false, stat);
		return stat.getVersion();

	}

	public List<ScheduleTaskItem> loadAllTaskItem(String taskType)
			throws Exception {
		List<ScheduleTaskItem> result = new ArrayList<ScheduleTaskItem>();
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_TaskItem;
		if (this.getZookeeper().exists(zkPath, false) == null)
			return result;
		List<String> taskitems = this.getZookeeper().getChildren(zkPath, false);
		// Collections.sort(taskItems);
		// 20150323 有些任务分片，业务方其实是用数字的字符串排序的。优先以数字进行排序，否则以字符串排序
		Collections.sort(taskitems, new Comparator<String>() {
			public int compare(String str1, String str2) {
				if (StringUtils.isNumeric(str1) && StringUtils.isNumeric(str2)) {
					int iU1 = Integer.parseInt(str1);
					int iU2 = Integer.parseInt(str2);
					if (iU1 == iU2) {
						return 0;
					} else if (iU1 > iU2) {
						return 1;
					} else {
						return -1;
					}
				} else {
					return str1.compareTo(str2);
				}
			}
		});

		// 封装ScheduleTaskItem，list结果返回
		for (String taskItem : taskitems) {
			ScheduleTaskItem info = new ScheduleTaskItem();
			info.setTaskItem(taskType);
			info.setTaskItem(taskItem);
			String zkTaskItemPath = zkPath + "/" + taskItem;
			byte[] curContent = this.getZookeeper().getData(
					zkTaskItemPath + "/cur_server", false, null);
			if (curContent != null) {
				info.setCurrentScheduleServer(new String(curContent));
			}

			byte[] reqContent = this.getZookeeper().getData(
					zkTaskItemPath + "/req_server", false, null);
			if (reqContent != null) {
				info.setRequestScheduleServer(new String(reqContent));
			}

			byte[] stsContent = this.getZookeeper().getData(
					zkTaskItemPath + "/sts", false, null);
			if (stsContent != null) {
				info.setSts(ScheduleTaskItem.TaskItemSts.valueOf(new String(
						stsContent)));
			}

			byte[] parameterContent = this.getZookeeper().getData(
					zkTaskItemPath + "/parameter", false, null);
			if (parameterContent != null) {
				info.setDealParameter(new String(parameterContent));
			}

			byte[] dealDescContent = this.getZookeeper().getData(
					zkTaskItemPath + "/deal_desc", false, null);
			if (dealDescContent != null) {
				info.setDealDesc(new String(dealDescContent));
			}
			result.add(info);
		}
		return result;
	}

	// 获取zk时间
	public long getSystemTime() {
		return this.zkBaseTime
				+ (System.currentTimeMillis() - this.localBaseTime);
	}

	public List<TaskItemDefine> reloadDealTaskItem(String taskType, String uuid)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public void releaseDealTaskItem(String taskType, String uuid)
			throws Exception {
		// TODO Auto-generated method stub

	}

	public int queryTaskItemCount(String taskType) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * 加载任务类型基本信息 从/baseTaskType下获取数据
	 */
	public ScheduleTaskType loadTaskTypeBaseInfo(String baseTaskType)
			throws Exception {
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType;
		if (this.getZookeeper().exists(zkPath, false) == null) {
			return null;
		}
		String valueString = new String(this.getZookeeper().getData(zkPath,
				false, null));
		ScheduleTaskType result = (ScheduleTaskType) this.gson.fromJson(
				valueString, ScheduleTaskType.class);
		return result;
	}

	/**
	 * 更新任务调度状态 给任务项下的节点sts deal_desc设置数据
	 */
	public void updateScheduleTaskItemStatus(String taskType, String taskItem,
			TaskItemSts sts, String message) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_TaskItem + "/" + taskItem;
		if (this.getZookeeper().exists(zkPath + "/sts", false) != null) {
			this.getZookeeper().setData(zkPath + "/sts",
					sts.toString().getBytes(), -1);
		}
		if (this.getZookeeper().exists(zkPath + "/deal_desc", false) != null) {
			if (message == null)
				message = "";
			this.getZookeeper().setData(zkPath + "/deal_desc",
					message.getBytes(), -1);
		}
	}

	public int clearExpireScheduleServer(String taskType, long expireTime)
			throws Exception {
		int result = 0;
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_Server;
		if (this.getZookeeper().exists(zkPath, false) == null) {
			String tempPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
					+ taskType;
			if (this.getZookeeper().exists(tempPath, false) == null) {
				this.getZookeeper().create(tempPath, null,
						this.zkManager.getAcl(), CreateMode.PERSISTENT);
			}
			this.getZookeeper().create(zkPath, null, this.zkManager.getAcl(),
					CreateMode.PERSISTENT);
		}
		for (String name : this.getZookeeper().getChildren(zkPath, false)) {
			try {
				Stat stat = this.getZookeeper().exists(zkPath + "/" + name,
						false);
				if (getSystemTime() - stat.getMtime() > expireTime) {
					ZKTools.deleteTree(getZookeeper(), zkPath + "/" + name);
					result++;
				}
			} catch (Exception e) {
				// 当有多台服务器时，存在并发清理的可能，忽略异常
				result++;
			}
		}
		return result;
	}

	public int clearTaskItem(String taskType, List<String> serverList)
			throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_TaskItem;
		int result = 0;
		for (String name : this.getZookeeper().getChildren(zkPath, false)) {
			// 获取当前任务执行的服务器
			byte[] curServerValue = this.getZookeeper().getData(
					zkPath + "/" + name + "/cur_server", false, null);
			if (curServerValue != null) {
				String curServer = new String(curServerValue);
				boolean isFind = false;
				for (String server : serverList) {
					if (curServer.equals(server)) {
						isFind = true;
						break;
					}
				}
				if (!isFind) {
					this.getZookeeper().setData(
							zkPath + "/" + name + "/cur_server", null, -1);
					result = result + 1;
				}
			} else {
				result = result + 1;
			}
		}
		return result;
	}

	public List<ScheduleServer> selectAllValidScheduleServer(String taskType)
			throws Exception {
		List<ScheduleServer> result = new ArrayList<ScheduleServer>();
		 String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		 String zkPath = this.PATH_BaseTaskType + "/" 
		 + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
		 if(this.getZookeeper().exists(zkPath, false) == null){
			 return result;
		 }
		 List<String> serverList = this.getZookeeper().getChildren(zkPath, false);
		 Collections.sort(serverList,new Comparator<String>(){

			public int compare(String u1, String u2) {
				return u1.substring(u1.lastIndexOf("$") + 1)
						.compareTo(u2.substring(u2.lastIndexOf("$") + 1));
			} 
		 });
		 //从所有的服务器节点上获取数据
		 for(String name : serverList){
			 try {
				String valueString = new String(this.getZookeeper().getData(zkPath + "/" + name,false,null));
			    ScheduleServer server = (ScheduleServer)this.gson.fromJson(valueString, ScheduleServer.class);
			    server.setCenterServerTime(new Timestamp(this.getSystemTime()));
			    result.add(server);
			 } catch (Exception e) {
				log.debug(e.getMessage(),e);
			}
		 }
		return result;
	}

	public List<String> loadScheduleServerNames(String taskType)
			throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/"  + baseTaskType + "/" 
		         + taskType + "/" + this.PATH_Server;
		if(this.getZookeeper().exists(zkPath, false) == null){
			return new ArrayList<String>();
		}
		List<String> serverList = this.getZookeeper().getChildren(zkPath, false);
		Collections.sort(serverList,new Comparator<String>() {

			public int compare(String u1, String u2) {
				return u1.substring(u1.lastIndexOf("$") + 1)
						.compareTo(u2.substring(u2.lastIndexOf("$") + 1));
				
			}
		});
		return serverList;
	}

	/**
	 * 分配任务
	 */
	public void assignTaskItem(String taskType, String currentUuid,
			int maxNumOfOneServer, List<String> taskServerList) throws Exception {
		if(this.isLeader(currentUuid, serverList)){
			
		}

	}

	public boolean refreshScheduleServer(ScheduleServer server)
			throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	public void registerScheduleServer(ScheduleServer server) throws Exception {
		// TODO Auto-generated method stub

	}

	public void unRegisterScheduleServer(String taskType, String serverUUID)
			throws Exception {
		// TODO Auto-generated method stub

	}

	public void clearExpireTaskTypeRunningInfo(String basetaskType,
			String serverUUID, double expireDateinterbal) throws Exception {
		// TODO Auto-generated method stub

	}

	public boolean isLeader(String uuid, List<String> serverList) {
		return uuid.equals(this.getLeader(serverList));
	}

	public void pauseAllServer(String baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	public void resumeAllServer(String baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * 获取所有的baseTaskType 遍历PATH_BaseTaskType，getData转化成ScheduleTaskType
	 */
	public List<ScheduleTaskType> getAllTaskTypeBaseInfo() throws Exception {
		String zkPath = this.PATH_BaseTaskType;
		List<ScheduleTaskType> result = new ArrayList<ScheduleTaskType>();
		List<String> names = this.getZookeeper().getChildren(zkPath, false);
		Collections.sort(names);
		for (String name : names) {
			result.add(this.loadTaskTypeBaseInfo(name));
		}
		return result;
	}

	public void clearTaskType(String baseTaskType) throws Exception {
		// 清除所有的Runtime TaskType
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType;
		List<String> list = this.getZookeeper().getChildren(zkPath, false);
		for (String name : list) {
			ZKTools.deleteTree(getZookeeper(), zkPath + "/" + name);
		}

	}

	public List<ScheduleTaskTypeRunningInfo> getAllTaskTypeRunningInfo(
			String baseTaskType) throws Exception {
		List<ScheduleTaskTypeRunningInfo> result = new ArrayList<ScheduleTaskTypeRunningInfo>();
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType;
		if (this.getZookeeper().exists(zkPath, false) == null) {
			return result;
		}
		List<String> list = this.getZookeeper().getChildren(zkPath, false);
		Collections.sort(list);

		for (String name : list) {

		}
		return null;
	}

	public void deleteTaskType(String baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	public List<ScheduleServer> selectScheduleServer(String baseTaskType,
			String ownSign, String ip, String orderStr) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public List<ScheduleServer> selectHistoryScheduleServer(
			String baseTaskType, String ownSign, String ip, String orderStr)
			throws Exception {
		List<ScheduleServer> result = new ArrayList<ScheduleServer>();
		for(String baseTaskType : this,getZookeeper()){
			
		}
		return null;
	}

	public List<ScheduleServer> selectScheduleServerByManagerFactoryUUID(
			String factoryUUID) throws Exception {
		List<ScheduleServer> result = new ArrayList<ScheduleServer>();
		for(String baseTaskType : this.getZookeeper().getChildren(this.PATH_BaseTaskType, false)){
			for(String taskType : this.getZookeeper().getChildren(this.PATH_BaseTaskType + "/" + baseTaskType, false)){
				String zkPath =  this.PATH_BaseTaskType+"/"+baseTaskType+"/"+taskType+"/"+this.PATH_Server;
				for(String uuid : this.getZookeeper().getChildren(zkPath, false)){
					String valueString = new String(this.getZookeeper().getData(zkPath + "/" + uuid, false, null));
					ScheduleServer server = (ScheduleServer) this.gson.fromJson(valueString, ScheduleServer.class);
				    server.setCenterServerTime(new Timestamp(this.getSystemTime()));
				    if(server.getManagerFactoryUUID().equals(factoryUUID)){
				    	result.add(server);
				    }
				}
			}
		}
		Collections.sort(result,new Comparator<ScheduleServer>(){

			public int compare(ScheduleServer u1, ScheduleServer u2) {
				int result = u1.getTaskType().compareTo(u2.getTaskType());
				if(result == 0){
					String s1 = u1.getUuid();
					String s2 = u2.getUuid();
					result = s1.substring(s1.lastIndexOf("$") + 1)
							.compareTo(s2.substring(s2.lastIndexOf("$")) + 1);
				}
				return result;
			}
			
		});
		return result;
	}

	/**
	 * 创建任务项，注意其中的CurrentServer 和RequestDServer不会起作用
	 */
	public void createScheduletaskItem(ScheduleTaskItem[] taskItems)
			throws Exception {
		for (ScheduleTaskItem taskItem : taskItems) {
			String zkPath = this.PATH_BaseTaskType + "/"
					+ taskItem.getBaseTaskType() + "/" + taskItem.getTaskType()
					+ this.PATH_TaskItem;
			if (this.getZookeeper().exists(zkPath, false) == null) {
				ZKTools.createPath(getZookeeper(), zkPath,
						CreateMode.PERSISTENT, this.zkManager.getAcl());
			}
			String zkTaskItemPath = zkPath + "/" + taskItem.getTaskItem();
			this.getZookeeper().create(zkTaskItemPath, null,
					this.zkManager.getAcl(), CreateMode.PERSISTENT);
			this.getZookeeper().create(zkTaskItemPath + "/cur_server", null,
					this.zkManager.getAcl(), CreateMode.PERSISTENT); // 没起作用
			this.getZookeeper().create(zkTaskItemPath + "/req_server", null,
					this.zkManager.getAcl(), CreateMode.PERSISTENT); // 没起作用
			this.getZookeeper().create(zkTaskItemPath + "/sts",
					taskItem.getSts().toString().getBytes(),
					this.zkManager.getAcl(), CreateMode.PERSISTENT);
			this.getZookeeper().create(zkTaskItemPath + "/parameter",
					taskItem.getDealParameter().getBytes(),
					this.zkManager.getAcl(), CreateMode.PERSISTENT);
			this.getZookeeper().create(zkTaskItemPath + "/deal_desc",
					taskItem.getDealDesc().getBytes(), this.zkManager.getAcl(),
					CreateMode.PERSISTENT);

		}

	}

	/**
	 * 根据基础配置里面的任务项来创建各个域里面的任务项
	 * 
	 * @param baseTaskType
	 * @param ownSign
	 * @param baseTaskItems
	 * @throws Exception
	 */
	public void createScheduleTaskItem(String baseTaskType, String ownSign,
			String[] baseTaskItems) throws Exception {
		ScheduleTaskItem[] taskItems = new ScheduleTaskItem[baseTaskItems.length];
		// 正则表达式需要深入研究
		Pattern p = Pattern.compile("\\s*:\\s*\\{");

		for (int i = 0; i < baseTaskItems.length; i++) {
			// 初始化任务队列类型 设置任务参数
			taskItems[i] = new ScheduleTaskItem();
			taskItems[i].setBaseTaskType(baseTaskType);
			taskItems[i].setTaskType(ScheduleUtil.getTaskTypeByBaseAndOwnSign(
					baseTaskType, ownSign));
			taskItems[i].setOwnSign(ownSign);
			Matcher matcher = p.matcher(baseTaskItems[i]);
			if (matcher.find()) {
				taskItems[i].setTaskItem(baseTaskItems[i].substring(0,
						matcher.start()).trim());
				taskItems[i].setDealParameter(baseTaskItems[i].substring(
						matcher.end(), baseTaskItems[i].length() - 1).trim());
			} else {
				taskItems[i].setTaskItem(baseTaskItems[i]);
			}
			taskItems[i].setSts(ScheduleTaskItem.TaskItemSts.ACTIVTE);
		}
		createScheduletaskItem(taskItems);
	}

	/**
	 * 删除任务项
	 */
	public void deleteScheduleTaskItem(String taskType, String taskItem)
			throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/"
				+ taskType + "/" + this.PATH_TaskItem + "/" + taskItem;
		ZKTools.deleteTree(getZookeeper(), zkPath);
	}

	public void setInitialRunningInfoSucuss(String baseTaskType,
			String taskType, String uuid) throws Exception {
		// TODO Auto-generated method stub

	}

	public String getLeader(List<String> serverList) {
		if(serverList == null || serverList.size() == 0){
			return "";
		}
		long no = Long.MAX_VALUE;
		long tempNo = -1;
		String leader = null;
		//遍历  获取最小的uuid
		for(String server : serverList){
			tempNo = Long.parseLong(server.substring(server.lastIndexOf("$")) + 1);
			if(no > tempNo){
				no = tempNo;
				leader = server;
			}
		}
		return leader;
	}
	
	

}

/**
 * 
 * @Title :日期序 列化/解序列化 实用工具类
 * @author gaojy
 * @INFO ： http://blog.csdn.net/itlwc/article/details/38454867
 * @Create_Date : 2017年8月24日上午11:02:31
 * @Update_Date :
 */
class TimestampTypeAdapter implements JsonSerializer<Timestamp>,
		JsonDeserializer<Timestamp> {

	public Timestamp deserialize(JsonElement json, Type typeOfT,
			JsonDeserializationContext context) throws JsonParseException {
		if (!(json instanceof JsonPrimitive)) {
			throw new JsonParseException("日期应该是一个String值");
		}
		try {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date data = (Date) format.parse(json.getAsString());
			return new Timestamp(data.getTime());
		} catch (Exception e) {
			throw new JsonParseException(e);
		}
	}

	public JsonElement serialize(Timestamp src, Type arg1,
			JsonSerializationContext arg2) {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateFormatAsString = format.format(new Date(src.getTime()));
		return new JsonPrimitive(dateFormatAsString);
	}

}
