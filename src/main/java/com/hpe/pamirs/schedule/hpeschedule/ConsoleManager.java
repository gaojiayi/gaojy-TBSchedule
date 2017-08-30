package com.hpe.pamirs.schedule.hpeschedule;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hpe.pamirs.schedule.hpeschedule.strategy.TBScheduleManagerFactory;

/**
 * 控制台管理
 * 
 * @author gaojy
 * 
 *
 */
public class ConsoleManager {

	protected static transient Log log = LogFactory
			.getLog(ConsoleManager.class);

	public final static String configFile = System.getProperty("user.dir")
			+ File.separator + "pamirsScheduleConfig.properties";
	
	private  static TBScheduleManagerFactory  scheduleManagerFactory;
	
	public static boolean isInitial() throws Exception{
		return scheduleManagerFactory != null;
	}
	
	public static boolean initial() throws Exception{
		if(scheduleManagerFactory != null)
			return true;
		File file = new File(configFile);
		scheduleManagerFactory = new TBScheduleManagerFactory();
		scheduleManagerFactory.start = false;
		
		if(file.exists()){
			//Console不启动调度能力
			Properties p = new Properties();
			FileReader reader = new FileReader(file);
			p.load(reader);
			reader.close();
			scheduleManagerFactory.init(p);
			log.info("加载Schedule配置文件：" +configFile );
			return true;
		}else{
			log.error("config 文件不村子啊！");
			return false;
		}
	}
}
