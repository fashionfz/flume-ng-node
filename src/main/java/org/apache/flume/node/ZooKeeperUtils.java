/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.node;

import java.net.InetAddress;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperUtils
{
  private static final String licencePath = "/agent/auth/data";
  private static final String agentNodePath = "/agent/node";
  public static ZooKeeper keeper;
  private static final Logger logger = LoggerFactory.getLogger(ZooKeeperUtils.class);
  
  

  public static String checkAuth(String zkConnectionString) {
    try {
      keeper = new ZooKeeper(zkConnectionString, 500000, new MyWatcher("/"));
      byte[] data = keeper.getData(licencePath, false, null);
      if (data == null) {
        logger.error("ITBA not run or ITBA not connected and init zookeeper !");
        return null;
      }
      int num = Integer.parseInt(new String(data));
      if (num == 0) {
        logger.error("agent auth number is 0 !");
        return null;
      }
      Stat st = keeper.exists(agentNodePath, null);
      if (st == null) {
        return createAgentPathAndNode();
      }
      List<String> child = keeper.getChildren(agentNodePath, true);
      if (num > child.size()) {
        return keeper.create(agentNodePath + "/", getServerIp().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      }

      logger.error("agent auth number is " + num + ", agent runing number is " + child.size());
      return null;
    }
    catch (Exception e) {
      logger.error(e.getMessage(), e);
    }return null;
  }

  public static void registerWatcher(String path)
  {
    try
    {
      keeper.exists(path, new MyWatcher(path));
    }
    catch (KeeperException e) {
      e.printStackTrace();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static String createAgentPathAndNode() throws KeeperException, InterruptedException {
    String[] dirs = agentNodePath.split("/");
    String tmp = "";
    for (int i = 0; i < dirs.length; i++) {
      if (StringUtils.isEmpty(dirs[i]))
        continue;
      tmp = tmp + "/" + dirs[i];
      Stat st = keeper.exists(tmp, null);
      if (st == null)
        keeper.create(tmp, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    return keeper.create(agentNodePath + "/", getServerIp().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  private static String getServerIp()
  {
    try {
      InetAddress addr = InetAddress.getLocalHost();
      return addr.getHostAddress();
    } catch (Exception e) {
      logger.error("获取agent IP错误:", e);
    }
    return "";
  }
  
  /**
   * 记录配置文件的使用者
   * @param config
   * @return
   */
  public static boolean recordConfigUsed(String config){
	  if(keeper == null)
		  return false;
	  try {
		String path = keeper.create(config + "/", getServerIp().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		if(StringUtils.isEmpty(path)){
			return false;
		}
		registerWatcher(path);
		return true;
	} catch (Exception e) {
		logger.error(e.getMessage(), e);
		return false;
	}
  }
}