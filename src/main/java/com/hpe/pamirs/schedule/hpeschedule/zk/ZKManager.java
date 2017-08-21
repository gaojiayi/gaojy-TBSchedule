package com.hpe.pamirs.schedule.hpeschedule.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;




/**
 * zookeeper 管理类
 * 
 * @author gaojy
 *
 */
public class ZKManager {
  private static transient Log log = LogFactory.getLog(ZKManager.class);
  private ZooKeeper zk;
  private List<ACL> acl = new ArrayList<ACL>();
  private Properties properties;
  private boolean isCheckParentPath = true;

  public enum keys {
    zkConnectString, rootPath, userName, password, zkSessionTimeout, isCheckParentPath
  }

  /*
   * public ZKManager(Properties aProperties) throws Exception { this.properties = aProperties;
   * String authString = this.properties.getProperty(keys.userName.toString()) + ":" +
   * this.properties.getProperty(keys.password.toString()); zk = new
   * ZooKeeper(this.properties.getProperty(keys.zkConnectString.toString()),
   * Integer.parseInt(this.properties.getProperty(keys.zkSessionTimeout.toString())), new
   * ScheduleWatcher(this)); // Add the specified scheme:auth information to this connection.
   * zk.addAuthInfo("digest", authString.getBytes());
   * 
   * // 改ACL 有所有操作权限 acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest",
   * DigestAuthenticationProvider .generateDigest(authString)))); // 只读权限 acl.add(new
   * ACL(ZooDefs.Perms.READ, Ids.ANYONE_ID_UNSAFE));
   * 
   * }
   */

  public ZKManager(Properties aProperties) throws Exception {
    this.properties = aProperties;
    this.connect();
  }

  // 重连zk

  public synchronized void reConnection() throws Exception {
    if (this.zk != null) {
      this.zk.close();
      this.zk = null;
      this.connect();
    }
  }

  private void connect() throws Exception {
    CountDownLatch connectionLatch = new CountDownLatch(1);
    createZookeeper(connectionLatch);
    connectionLatch.await(10, TimeUnit.SECONDS);
  }

  private void createZookeeper(final CountDownLatch connectonLatch) throws Exception {
    zk =
        new ZooKeeper(this.properties.getProperty(keys.zkConnectString.toString()),
            Integer.parseInt(this.properties.getProperty(keys.zkSessionTimeout.toString())),
            new Watcher() {

              
              public void process(WatchedEvent event) {
                sessionEvent(connectonLatch, event);

              }

            });
    String authString =
        this.properties.getProperty(keys.userName.toString()) + ":"
            + this.properties.getProperty(keys.password.toString());
    this.isCheckParentPath =
        Boolean
            .parseBoolean(this.properties.getProperty(keys.isCheckParentPath.toString(), "true"));
    zk.addAuthInfo("digest", authString.getBytes());
    acl.clear();
    acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider
        .generateDigest(authString))));
    acl.add(new ACL(ZooDefs.Perms.READ, Ids.ANYONE_ID_UNSAFE));
  }

  private void sessionEvent(CountDownLatch connectionLatch, WatchedEvent event) {
    if (event.getState() == KeeperState.SyncConnected) {
      log.info("收到ZK连接成功事件！");
      // 释放计数器
      connectionLatch.countDown();
    } else if (event.getState() == KeeperState.Expired) {
      log.error("会话超时，等待重新建立ZK连接...");
      try {
        reConnection();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    } // Disconnected：Zookeeper会自动处理Disconnected状态重连
    else if (event.getState() == KeeperState.Disconnected) {
      log.info("tb_hj_schedule Disconnected，等待重新建立ZK连接...");
      try {
        reConnection();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    } else if (event.getState() == KeeperState.NoSyncConnected) {
      log.info("tb_hj_schedule NoSyncConnected，等待重新建立ZK连接...");
      try {
        reConnection();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    } else {
      log.info("tb_hj_schedule 会话有其他状态的值，event.getState() =" + event.getState() + ", event  value="
          + event.toString());
      connectionLatch.countDown();
    }
  }

  public static Properties createProperties() {
    Properties result = new Properties();
    result.setProperty(keys.zkConnectString.toString(), "localhost:2181");
    result.setProperty(keys.rootPath.toString(), "/taobao-pamirs-schedule/huijin");
    result.setProperty(keys.userName.toString(), "ScheduleAdmin");
    result.setProperty(keys.password.toString(), "password");
    // The negotiated session timeout for this ZooKeeper client instance.
    result.setProperty(keys.zkSessionTimeout.toString(), "60000");
    result.setProperty(keys.isCheckParentPath.toString(),"true");

    return result;
  }

  public String getRootPath() {
    return this.properties.getProperty(keys.rootPath.toString());
  }

  public String getConnectStr() {
    return this.properties.getProperty(keys.zkConnectString.toString());
  }

  public boolean checkZookeeperState() {
    return zk.getState().isAlive() && (zk.getState() == States.CONNECTED);
  }

  public void initial() throws Exception {
    // 当zk状态正常后才能调用
    if (zk.exists(this.getRootPath(), false) == null) {
      // 不存在根目录
      ZKTools.createPath(zk, this.getRootPath(), CreateMode.PERSISTENT, acl);
      if(isCheckParentPath == true){
        checkParent(zk,this.getRootPath());
      }
      // 设置版本信息
      zk.setData(this.getRootPath(), Version.getVersion().getBytes(), -1);
    } else {
      // 初始化时已存在目录
      // 先校验父亲节点，本身是否是schedule的目录
      if(isCheckParentPath == true){
        checkParent(zk,this.getRootPath());
      }
      byte[] value = zk.getData(this.getRootPath(), false, null);
      if (value == null) {
        zk.setData(this.getRootPath(), Version.getVersion().getBytes(), -1);
      } else {
        String dataVersion = new String(value);
        if (!Version.isCompatible(dataVersion)) {
          throw new Exception("Pamirs-Schedule程序版本 " + Version.getVersion()
              + " 不兼容Zookeeper中的数据版本 " + dataVersion);
        }
        log.info("当前的程序版本:" + Version.getVersion() + " 数据版本: " + dataVersion);
      }
    }
  }

  /**
   * 检查根目录是否合法,子目录中的data不能存在taobao-pamirs-schedule-
   * 
   * @param zk
   * @param path
   * @throws Exception
   */
  public static void checkParent(ZooKeeper zk, String path) throws Exception {
    String[] list = path.split("/");
    String zkPath = "";
    for (int i = 0; i < list.length - 1; i++) {
      String str = list[i];
      if (StringUtils.isNotEmpty(str)) {
        zkPath = zkPath + "/" + str;
        if (zk.exists(zkPath, false) != null) {
          byte[] value = zk.getData(zkPath, false, null);
          if (value != null) {
            String tmpVersion = new String(value);
            if (tmpVersion.indexOf("taobao-pamirs-schedule-") >= 0) {
              throw new Exception(
                  "\""
                      + zkPath
                      + "\"  is already a schedule instance's root directory, its any subdirectory cannot as the root directory of others");
            }
          }
        }
      }
    }
  }


  public void close() throws InterruptedException {
    log.info("关闭zookeeper连接");
    if(zk == null) {
        return;
    }
    this.zk.close();
}
  public List<ACL> getAcl() {
    return acl;
  }

  public ZooKeeper getZookeeper() throws Exception {
    if (this.checkZookeeperState() == false) {
      throw new Exception("Zookeeper[" + this.getConnectStr() + "] connect error");
    }

    return this.zk;
  }
}
