package com.hpe.pamirs.schedule.hpeschedule;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * 
 * @Title :单个任务处理实现
 * @author gaojy
 *
 * @Create_Date : 2017年9月12日下午5:06:20
 * @Update_Date :
 */
public class DemoTaskBean implements IScheduleTaskDealSingle<Long>{

  protected static transient Log log = LogFactory.getLog(DemoTaskBean.class);
  
  //创建计较器
  public Comparator<Long> getComparator() {
    return new Comparator<Long>(){

      public int compare(Long o1, Long o2) {
        return o1.compareTo(o2);
      }
      
      public boolean equals(Object obj){
        return this == obj;
      }
    };
  }
  
  public List<Long> selectTask(String taskParameter, String ownSign, int taskItemNum,
      List<TaskItemDefine> queryCondition, int fetchNum) throws Exception {
    List<Long> result = new ArrayList<Long>();
    int num = fetchNum / queryCondition.size();
    Random random =  new Random(System.currentTimeMillis());
    String message = "获取数据...【ownSign=" + ownSign + ",taskParameter=\""+taskParameter+"\"]:";
    boolean isFirst = true;
    for(TaskItemDefine s : queryCondition){
      long taskItem = Integer.parseInt(s.getTaskItemId()) * 10000000L;
      for(int i = 0; i < num ; i++){
        result.add(taskItem + random.nextLong() % 100000L)
      }
    }
    log.info(message);
    return result;
  }



  public boolean execute(Long task, String ownSign) throws Exception {
    Thread.sleep(50);
    log.info("处理任务【" + ownSign + "]:" + task);
    return true;
  }

}
