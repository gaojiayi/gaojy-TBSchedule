package com.hpe.pamirs.schedule.hpeschedule;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Test;

import com.hpe.pamirs.schedule.hpeschedule.zk.ScheduleWatcher;
import com.hpe.pamirs.schedule.hpeschedule.zk.ZKTools;
/**
 * http://blog.csdn.net/paul342/article/details/52191272
 * @author Administrator
 *
 */
public class ZKTest {

	private static final String connectString = "192.168.196.10:2181";
	/**
	 * 验证 zk打印目录树  此处我使用了递归
	 * @throws Exception
	 */
	@Test
	public void testCloseStatus() throws Exception {
		ZooKeeper zk = new ZooKeeper(connectString, 3000,
				new ScheduleWatcher(null));
		int i = 1;
		while (true) {
			try {
				StringWriter writer = new StringWriter();
				ZKTools.printTree(zk, "/zookeeper/quota", writer, "");
				System.out
						.println(i++ + "----" + writer.getBuffer().toString());
				Thread.sleep(2000);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
	
	
        /*	2----/[v.0][]
			/taobao-pamirs-schedule
			/taobao-pamirs-schedule/gaojy[v.1][taobao-pamirs-schedule-3.0.0]
			/taobao-pamirs-schedule/gaojy/strategy
			/taobao-pamirs-schedule/gaojy/baseTaskType
			/taobao-pamirs-schedule/gaojy/factory
			/zookeeper[v.0][]
			/zookeeper/quota[v.0][]
			/zookeeper/quota/abc[v.0][]
			/zookeeper/quota/gaojy[v.0][]
			/zookeeper/quota/gaojy/A2[v.0][]
			/zookeeper/quota/gaojy/A1[v.0][]
			/zookeeper/quota/gaojy/A3[v.0][]*/
	@Test
	public void printTree() throws Exception {
		ZooKeeper zk = new ZooKeeper(connectString, 3000,
				new ScheduleWatcher(null));
		int i = 1;
		while (true) {
			try {
				StringWriter writer = new StringWriter();
				ZKTools.printTree(zk, "/", writer, "");
				System.out
						.println(i++ + "----" + writer.getBuffer().toString());
				Thread.sleep(2000);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
	
	/**
	 * 删除/zookeeper 目录及其子目录
	 * @throws Exception
	 */
	 @Test
	 public void deletePath() throws Exception{
		 ZooKeeper zk = new ZooKeeper(connectString, 30000,
				 new  ScheduleWatcher(null));
		 zk.addAuthInfo("digest","TestUser:password".getBytes());
		 ZKTools.deleteTree(zk,"/zookeeper/quota/abc");
		 StringWriter writer = new StringWriter();
		 ZKTools.printTree(zk, "/", writer,"\n");
		 System.out.println(writer.getBuffer().toString());
	 } 
	 
		@Test
		public void testACL() throws Exception {
			ZooKeeper zk = new ZooKeeper("192.168.196.10:2181", 3000,new  ScheduleWatcher(null));
			List<ACL> acls = new ArrayList<ACL>();
			zk.addAuthInfo("digest","TestUser:password".getBytes());
			acls.add(new ACL(ZooDefs.Perms.ALL,new Id("digest",DigestAuthenticationProvider.generateDigest("TestUser:password"))));
			acls.add(new ACL(ZooDefs.Perms.READ,Ids.ANYONE_ID_UNSAFE));
			zk.create("/abc", new byte[0], acls, CreateMode.PERSISTENT);
			zk.getData("/abc",false,null);
		}
}
