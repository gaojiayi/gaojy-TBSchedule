package com.hpe.pamirs.schedule.hpeschedule;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 多任务处理实现
 * 
 * @author Administrator
 *
 */
public class DemoTaskBeanMulti implements IScheduleTaskDealMulti<Long> {

	protected static transient Log log = LogFactory
			.getLog(DemoTaskBeanMulti.class);

	public List<Long> selectTask(String taskParameter, String ownSign,
			int taskItemNum, List<TaskItemDefine> queryCondition, int fetchNum)
			throws Exception {
		List<Long> result = new ArrayList<Long>();
		int num = fetchNum / queryCondition.size();
		Random random = new Random(System.currentTimeMillis());
		String message = "获取数据...[ownSign=" + ownSign + ",taskParameter=\""
				+ taskParameter + "\"]:";
		boolean isFirst = true;
		for (TaskItemDefine s : queryCondition) {
			long taskItem = Long.parseLong(s.getTaskItemId()) * 10000000L;
			for (int i = 0; i < num; i++) {
				result.add(taskItem + random.nextLong() % 100000L);
			}
			if (isFirst == false) {
				message = message + ",";
			} else {
				isFirst = false;
			}
			message = message + s.getTaskItemId() + "{" + s.getParameter()
					+ "}";
		}
		log.info(message);
		return result;
	}

	public Comparator<Long> getComparator() {
		return new Comparator<Long>() {
			public int compare(Long o1, Long o2) {
				return o1.compareTo(o2);
			}

			public boolean equals(Object obj) {
				return this == obj;
			}
		};
	}

	public boolean execute(Long[] tasks, String ownSign) throws Exception {
		Thread.sleep(50);
		return true;
	}

}
