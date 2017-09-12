package com.hpe.pamirs.schedule.hpeschedule;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.unitils.UnitilsJUnit4;
import org.unitils.spring.annotation.SpringApplicationContext;
import org.unitils.spring.annotation.SpringBeanByName;

import com.hpe.pamirs.schedule.hpeschedule.strategy.ScheduleStrategy;
import com.hpe.pamirs.schedule.hpeschedule.strategy.TBScheduleManagerFactory;
import com.hpe.pamirs.schedule.hpeschedule.taskmanager.ScheduleTaskType;

/**
 * 主要是创建目录并设置数据    相当于 在console中页面配置数据
 * @Title :初始化数据 
 * @author gaojy
 *
 * @Create_Date : 2017年9月12日下午12:18:43
 * @Update_Date :
 */
@SpringApplicationContext({"schedule.xml"})
public class InitialDemoConfigData extends UnitilsJUnit4 {

  protected static transient Log log = LogFactory.getLog(InitialDemoConfigData.class);

  @SpringBeanByName
  TBScheduleManagerFactory scheduleManagerFactory;

  public void setScheduleManagerFactory(TBScheduleManagerFactory tbScheduleManagerFactory) {
    this.scheduleManagerFactory = tbScheduleManagerFactory;
  }

  @Test
  public void initialConfigData() throws Exception {
    String baseTaskTypeName = "DemoTask";
    // zk 没有完成初始化 等待1s
    while (!this.scheduleManagerFactory.isZookeeperInitialSuccess()) {
      Thread.sleep(1000);
    }
    scheduleManagerFactory.stopServer(null);
    Thread.sleep(1000);
    try {
      // 删除 /更目录/baseTaskType/DemoTask
      this.scheduleManagerFactory.getScheduleDataManager().deleteTaskType(baseTaskTypeName);
    } catch (Exception e) {
      // TODO: handle exception
    }

    // 创建任务调度DemoTask的基本信息
    ScheduleTaskType baseTaskType = new ScheduleTaskType();
    baseTaskType.setBaseTaskType(baseTaskTypeName);
    baseTaskType.setDealBeanName("demoTaskBean");
    baseTaskType.setHeartBeatRate(2000);
    baseTaskType.setJudgeDeadInterval(10000);
    baseTaskType.setTaskParameter("AREA=杭州,YEAR>30");
    baseTaskType.setTaskItems(ScheduleTaskType
        .splitTaskItem("0:{TYPE=A,KIND=1},1:{TYPE=A,KIND=2},2:{TYPE=A,KIND=3},3:{TYPE=A,KIND=4},"
            + "4:{TYPE=A,KIND=5},5:{TYPE=A,KIND=6},6:{TYPE=A,KIND=7},7:{TYPE=A,KIND=8},"
            + "8:{TYPE=A,KIND=9},9:{TYPE=A,KIND=10}"));
    this.scheduleManagerFactory.getScheduleDataManager().createBaseTaskType(baseTaskType);
    log.info("创建调度任务成功：" + baseTaskType.toString());

    //创建任务DemoTask的调度
    String taskName = baseTaskTypeName + "$TEST";
    String  strategyName = baseTaskTypeName + "-Strategy";
    
    try {
      //删除      更目录/strategy/DemoTask-Strategy
      this.scheduleManagerFactory.getScheduleStrategyManager()
        .deleteMachineStrategy(strategyName,true);
    } catch (Exception e) {
       e.printStackTrace();
    }
    
    ScheduleStrategy  strategy = new ScheduleStrategy();
    strategy.setStrategyName(strategyName);
    strategy.setKind(ScheduleStrategy.Kind.Schedule);
    strategy.setTaskName(taskName);
    strategy.setTaskParameter("中国");
    
    strategy.setNumOfSingleServer(1);
    strategy.setAssignNum(10);
    strategy.setIPList("127.0.0.1".split(","));
    
    //创建目录  并设置数据
    this.scheduleManagerFactory.getScheduleStrategyManager()
      .createScheduleStrategy(strategy);
    log.info("创建调度策略成功：" + strategy.toString());
  }
  
  
  
  

/*  0:{TYPE=A,KIND=1}
  1:{TYPE=A,KIND=2}
  2:{TYPE=A,KIND=3}
  3:{TYPE=A,KIND=4}
  4:{TYPE=A,KIND=5}
  5:{TYPE=A,KIND=6}
  6:{TYPE=A,KIND=7}
  7:{TYPE=A,KIND=8}
  8:{TYPE=A,KIND=9}
  9:{TYPE=A,KIND=10}*/
  public static void main(String args[]) {
    String[] splitTaskItem =
        ScheduleTaskType
            .splitTaskItem("0:{TYPE=A,KIND=1},1:{TYPE=A,KIND=2},2:{TYPE=A,KIND=3},3:{TYPE=A,KIND=4},"
                + "4:{TYPE=A,KIND=5},5:{TYPE=A,KIND=6},6:{TYPE=A,KIND=7},7:{TYPE=A,KIND=8},"
                + "8:{TYPE=A,KIND=9},9:{TYPE=A,KIND=10}");
    for (String str : splitTaskItem) {
      System.out.println(str);
    }

  }
}
