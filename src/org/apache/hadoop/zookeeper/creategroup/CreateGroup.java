package org.apache.hadoop.zookeeper.creategroup;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class CreateGroup implements Watcher {

	private static final int SESSION_TIMEOUT = 5000;
	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException {
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
	}

	@Override
	public void process(WatchedEvent event) { // Watcher interface
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}

	public void create(String groupName) throws KeeperException,
			InterruptedException {
		String path = "/" + groupName;
		String createdPath = zk.create(path, null/* data,the contents of the znode should be a byte array, null here */,
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		/*An ephemeral znode will be deleted by the
ZooKeeper service when the client that created it disconnects, either by explicitly disconnecting
or if the client terminates for whatever reason. A persistent znode, on the
other hand, is not deleted when the client disconnects.*/
		
		System.out.println("Created " + createdPath);
	}

	public void close() throws InterruptedException {
		zk.close();
	}

	public static void main(String[] args) throws Exception {
		CreateGroup createGroup = new CreateGroup();
		createGroup.connect("10.35.62.195");
		createGroup.create("zoo");
		createGroup.close();
	}
}
