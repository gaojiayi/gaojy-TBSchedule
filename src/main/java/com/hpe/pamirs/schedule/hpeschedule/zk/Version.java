package com.hpe.pamirs.schedule.hpeschedule.zk;
/**
 * 版本
 * @author gaojy
 *
 */
public class Version {

  public static final String version = "tbschedule-3.2.12";
  
  public static String getVersion(){
    return version;
  }
  public static boolean isCompatible(String dataVersion){
    if(version.compareTo(dataVersion) >= 0){
      //本地数据版本  >=  zk上的数据版本, 返回true
      return true;
    }else{
      return false;
    }
  }
}
