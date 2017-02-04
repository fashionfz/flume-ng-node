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

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyWatcher implements Watcher {
	private static final Logger logger = LoggerFactory
			.getLogger(MyWatcher.class);
	private String path;

	public MyWatcher(String path) {
		this.path = path;
	}

	public void process(WatchedEvent event) {
		try {
			if ((event.getPath() != null) && (event.getType() != Watcher.Event.EventType.None))
				ZooKeeperUtils.keeper.exists(this.path, new MyWatcher(this.path));
		} catch (Exception e) {
			logger.error("注册watcher失败:", e);
		}
		logger.info("zookeeper node change :" + event.getPath() + "-" + event.getType().toString());
		if (StringUtils.isEmpty(this.path))
			return;
		if ((this.path.equals(event.getPath())) && (event.getType() == Watcher.Event.EventType.NodeDeleted))
			System.exit(0);
	}

	public static void main(String[] args) {
		String path = ZooKeeperUtils.checkAuth("192.168.1.166:2181");
		System.out.println(path);
		ZooKeeperUtils.registerWatcher(path);
		try {
			Thread.sleep(1000000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}