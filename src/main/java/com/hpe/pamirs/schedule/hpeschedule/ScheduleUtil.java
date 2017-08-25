package com.hpe.pamirs.schedule.hpeschedule;

import java.net.Inet4Address;
import java.net.InetAddress;
/**
 * 
 * @Title :解析任务类型  IP  工具类
 * @author gaojy
 *
 * @Create_Date : 2017年8月25日下午3:20:17
 * @Update_Date :
 */
public class ScheduleUtil {
  public static final String OWN_SIGN_BASE = "BASE";

  public static String getTaskTypeByBaseAndOwnSign(String baseType, String ownSign) {
    if (ownSign.equals(OWN_SIGN_BASE) == true) {
      return baseType;
    }
    return baseType + "$" + ownSign;

  }

  public static String getLocalIP() {
    try {
      return Inet4Address.getLocalHost().getHostAddress();
    } catch (Exception e) {
      return "";
    }
  }

  public static String getLocalHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "";
    }
  }

  public static String splitBaseTaskTypeFromType(String taskType) {
    if (taskType.indexOf("$") >= 0) {
      return taskType.substring(0, taskType.indexOf("$"));
    } else {
      return taskType;
    }
  }

  public static String splitOwnsignFromTaskType(String taskType) {
    if (taskType.indexOf("$") >= 0) {
      return taskType.substring(taskType.indexOf("$") + 1);
    } else {
      return OWN_SIGN_BASE;
    }
  }
}
