package com.hpe.pamirs.schedule.hpeschedule.zk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * 定义zookeeper事件
 * 
 * @author gaojy
 *
 */
public class ScheduleWatcher implements Watcher {

  private static transient Log log = LogFactory.getLog(ScheduleWatcher.class);

  private Map<String, Watcher> route = new ConcurrentHashMap<String, Watcher>();

  private ZKManager manager;

  public ScheduleWatcher(ZKManager aManager) {
    this.manager = aManager;
  }

  public void registerChildrenChanged(String path, Watcher watcher) throws Exception {
    // 当注册一个path 设置时间
    manager.getZookeeper().getChildren(path, true);
    route.put(path, watcher);
  }

  
  public void process(WatchedEvent event) {
    log.info("已经触发了" + event.getType() + "事件！" + event.getPath());

    if (event.getType() == Event.EventType.NodeChildrenChanged) {
      String path = event.getPath();
      Watcher watcher = route.get(path);
      if (watcher != null) {
        try {
          // 如果存在自定义的watcher 则统一处理
          watcher.process(event);
        } finally {
          try {
            if (manager.getZookeeper().exists(path, null) != null) {
              // 由于 zk的watcher触发一次后不会再次触发，此处重新监听事件
              manager.getZookeeper().getChildren(path, true);
            }
          } catch (Exception e) {
            log.error(path + ":" + e.getMessage(), e);
          }
        }
      } else {
        log.info("已经触发了" + event.getType() + ":" + event.getState() + "事件！" + event.getPath());
      }
    } else if (event.getState() == KeeperState.AuthFailed) {
      log.info("tb_hj_schedule zk status =KeeperState.AuthFailed！");

    } else if (event.getState() == KeeperState.Disconnected) {
      // 断开重连
      log.info("tb_hj_schedule zk status =KeeperState.Disconnected！");
      try {
        manager.reConnection();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    } else if (event.getState() == KeeperState.NoSyncConnected) {
      log.info("tb_hj_schedule zk status =KeeperState.NoSyncConnected！等待重新建立ZK连接.. ");
      try {
        manager.reConnection();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    } else if (event.getState() == KeeperState.Unknown) {
      log.info("tb_hj_schedule zk status =KeeperState.Unknown！");
    } else if (event.getState() == KeeperState.Expired) {
      log.error("会话超时，等待重新建立ZK连接...");
      try {
        manager.reConnection();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }
  }
}
