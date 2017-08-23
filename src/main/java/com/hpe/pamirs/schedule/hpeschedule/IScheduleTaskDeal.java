package com.hpe.pamirs.schedule.hpeschedule;

import java.util.Comparator;
import java.util.List;


/**
 * 
 * @Title :调度器对外的基础接口
 * @author gaojy
 *
 * @Create_Date : 2017年8月23日下午4:21:07
 * @Update_Date :
 */
public interface  IScheduleTaskDeal<T> {

  /**
   * 根据条件，查询当前调度器可处理的任务
   * @param taskParameter   任务的自定义参数
   * @param ownSign  当前环境名称
   * @param taskItemNum  当前任务类型的任务队列数量
   * @param taskItemList  当前调度服务器，分配到的可处理队列
   * @param eachFetchDataNum  每次获取数据的数量
   * @return
   * @throws Exception
   */
  public List<T> selectTask(String taskParameter,String ownSign,
      int taskItemNum,List<TaskItemDefine> taskItemList,int eachFetchDataNum) throws Exception;
  
  /**
   * 获取任务的比较器，主要在NotSleep模式下需要用到
   * @return
   */
  public Comparator<T> getComparator();
  
}
