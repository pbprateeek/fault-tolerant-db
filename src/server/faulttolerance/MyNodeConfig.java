package server.faulttolerance;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MyNodeConfig {
    private List<String> nodeIDs;
    private ZooKeeper zookeeper;
    private String configPath = "/MyNodeConfig";

    public MyNodeConfig(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
        this.nodeIDs = new ArrayList<>();
    }

    public synchronized void addNodeID(String nodeID) {
        if (!nodeIDs.contains(nodeID)) {
            nodeIDs.add(nodeID);
        }
    }

    public synchronized boolean removeNodeID(String nodeID) {
        return nodeIDs.remove(nodeID);
    }

    public synchronized List<String> getNodeIDs() {
        return new ArrayList<>(nodeIDs);
    }

    public synchronized boolean containsNodeID(String nodeID) {
        return nodeIDs.contains(nodeID);
    }

    // Method to save to Zookeeper
    public synchronized void saveToZookeeper() throws KeeperException, InterruptedException {
        byte[] data = String.join(",", nodeIDs).getBytes();
        if (zookeeper.exists(configPath, false) == null) {
            zookeeper.create(configPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zookeeper.setData(configPath, data, -1);
        }
    }

    // Method to load from Zookeeper
    public synchronized void loadFromZookeeper() throws KeeperException, InterruptedException {
        if (zookeeper.exists(configPath, false) != null) {
            byte[] data = zookeeper.getData(configPath, false, null);
            nodeIDs = new ArrayList<>(Arrays.asList(new String(data).split(",")));
            System.out.println("Loaded node IDs from Zookeeper: " + nodeIDs);
        } else {
            System.out.println("No node configuration found in Zookeeper.");
        }
    }


}

