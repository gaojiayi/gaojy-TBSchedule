package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @Title :调度任务类型
 * @author gaojy
 *
 * @Create_Date : 2017年8月22日下午5:07:35
 * @Update_Date :
 */
public class ScheduleTaskType implements java.io.Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = -1L;

  /**
   * 任务类型
   */
  private String baseTaskType; 
  
  /**
   * 向配置中心更新心跳信息的频率
   */
  private long heartBeatRate = 5*1000; //1分钟
  
  /**
   * 判断一个服务器死亡的周期。为了安全，至少是心跳周期的两倍以上
   */
  private long judgeDeadInterval = 1*60*1000;//2分钟
  
  /**
   * 当没有数据的时候，休眠的时间
   */
  private int sleepTimeNoData = 500;
  
  /**
   * 每一次数据处理完后休眠的时间
   */
  private int sleepTimeInterval = 0 ;
  
 /**
  * 每次获取数据的数量
  * 
  */
  private int fethDataNumber = 500;
  
  /**
   * 在批处理的时候，每次处理的数据量
   */
  private int executeNumber =1;
  
  private int threadNumber = 5;
  
  /**
   * 调度器类型
   */
  private String processorType = "SLEEP";
  
  /**
   * 循序执行的开始时间
   */
  private String permitRunStartTime;
  
  /**
   * 允许执行的结束时间
   */
  private String permitRunEndTime;
  
  /**
   * 清除过期环境信息的时间间隔,以天为单位
   */
  private double expireOwnSignInterval = 1;
  
  /**
   * 处理任务的BeanName
   */
  private String dealBeanName;
  /**
   * 任务bean的参数，由用户自定义格式的字符串
   */
  private String taskParameter;
  
  //任务类型：静态static,动态dynamic
  private String taskKind = TASKKIND_STATIC;
  
  public static String TASKKIND_STATIC="static";
  public static String TASKKIND_DYNAMIC="dynamic";
  
  
  /**
   * 任务项数组
   */
  private String[] taskItems;
  
  /**
   * 每个线程组能处理的最大任务项目书目
   */
  private int maxTaskItemsOfOneThreadGroup = 0;
  /**
   * 版本号
   */
  private long version;
  
  /**
   * 服务状态: pause,resume
   */
  private String sts = STS_RESUME;
  
  public static String STS_PAUSE="pause";
  public static String STS_RESUME="resume";

  public static String[] splitTaskItem(String str){
    List<String> list = new ArrayList<String>();
    int start = 0;
    int index = 0;
    while(index < str.length()){
      
    }
  }
}
