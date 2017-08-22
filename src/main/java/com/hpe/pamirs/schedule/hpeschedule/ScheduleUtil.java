package com.hpe.pamirs.schedule.hpeschedule;

import java.net.Inet4Address;
import java.net.InetAddress;

public class ScheduleUtil {
  public static final  String OWN_SIGN_BASE ="BASE";
  public static String getTaskTypeByBaseAndOwnSign(String baseType,String ownSign){
   if(ownSign.equals(OWN_SIGN_BASE) == true){
     return baseType;
   }
    return baseType+"$" + ownSign;
    
  }
   
  public static String getLocalIP(){
    try{
      return Inet4Address.getLocalHost().getHostAddress();
    }catch(Exception e){
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
}
