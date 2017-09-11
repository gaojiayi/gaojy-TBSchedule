package com.hpe.pamirs.schedule.hpeschedule.zk;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeperMain;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * zk 工具类 用于创建目录 查找目录树
 * 
 * @author gaojy
 *
 */
public class ZKTools {

	public static void createPath(ZooKeeper zk, String Path,
			CreateMode createMode, List<ACL> acl) throws Exception {
		// 解析路径
		String[] list = Path.split("/");
		String zkPath = "";
		for (String str : list) {
			// 逐层校验 不存在则创建目录
			if (StringUtils.isNotEmpty(str)) {
				zkPath = zkPath + "/" + str;
				if (zk.exists(zkPath, false) == null) {
					zk.create(zkPath, null, acl, createMode);
				}
			}
		}
	}

	// 打印节点
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void printTree(ZooKeeper zk, String path, Writer writer,
			String lineSplitChar) throws Exception {
		// String[] list = getTree(zk, path);
		// update by gaojy

		List lists = new ArrayList();
		getTree(zk, path, lists);
		String[] list = (String[]) lists.toArray(new String[0]);
		Stat stat = new Stat();
		for (String name : list) {
			byte[] value = zk.getData(name, false, stat);
			if (value == null) {
				writer.write(name + lineSplitChar);
			} else {
				writer.write(name + "[v." + stat.getVersion() + "]" + "["
						+ new String(value) + "]" + lineSplitChar);
			}
		}

	}

	// 删除目标目录 并且循环删除子目录
	public static void deleteTree(ZooKeeper zk, String path) throws Exception {
		String[] list = getTree(zk, path);
		for (int i = list.length - 1; i >= 0; i--) {
			zk.delete(list[i], -1);
		}
	}

	// 我觉得这个方法有问题 应该使用递归
	public static String[] getTree(ZooKeeper zk, String path) throws Exception {
		if (zk.exists(path, false) == null) {
			return new String[0];
		}
		List<String> dealList = new ArrayList<String>();
		dealList.add(path);
		int index = 0;
		while (index < dealList.size()) {
			String tempPath = dealList.get(index);
			List<String> children = zk.getChildren(tempPath, false);
			if (!tempPath.equalsIgnoreCase("/")) {
				tempPath = tempPath + "/";
			}
			Collections.sort(children);
			for (int i = children.size() - 1; i >= 0; i--) {
				dealList.add(index + 1, tempPath + children.get(i));
			}
			index++;
		}
		return (String[]) dealList.toArray(new String[0]);
	}

	/**
	 * 使用递归遍历所有结点
	 * 
	 * @param zk
	 * @param path
	 * @param dealList
	 * @throws Exception
	 * @throws InterruptedException
	 */
	private static void getTree(ZooKeeper zk, String path, List<String> dealList)
			throws Exception, InterruptedException {
		//添加父目录
		dealList.add(path);
		List<String> children = zk.getChildren(path, false);
		if (path.charAt(path.length() - 1) != '/') {
			path = path + "/";
		}
		//添加子目录
		for (int i = 0; i < children.size(); i++) {
			// dealList.add(path+children.get(i));
			getTree(zk, path + children.get(i), dealList);
		}

	}

	/**
	 * 获取指定目录下的所有子目录
	 * 
	 * @param zk
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static String[] getAllTree(ZooKeeper zk, String path)
			throws Exception {
		LinkedList dealList = new LinkedList();
		getTree(zk, path, dealList);
		return (String[]) dealList.toArray(new String[0]);
	}
}
