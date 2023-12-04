package server.faulttolerance;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MyNodeConfig {
    private List<String> nodeIDs;
    private static final Object zookeeperLock = new Object();
    private static final String CONFIG_FILE_PATH = "myNodeConfig.txt";
    private static final String CONFIG_LOCK_FILE_PATH = "myNodeConfig.lock";

    private final Object fileLock = new Object();


    public MyNodeConfig() {
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
    public void saveToFile(MyNodeConfig myNodeConfig) throws IOException {
        synchronized (fileLock) {
            // Acquire file lock
            try (FileChannel channel = FileChannel.open(Paths.get(CONFIG_LOCK_FILE_PATH), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 FileLock lock = channel.lock()) {

               nodeIDs = myNodeConfig.getNodeIDs();

                // Save updated list back to file
                List<String> lines = Collections.singletonList(String.join(",", nodeIDs));
                Path filePath = Paths.get(CONFIG_FILE_PATH);
                Files.write(filePath, lines, StandardCharsets.UTF_8);

                System.out.println("Updated MyNodeConfig saved to file: " + nodeIDs);
            }
        }
    }


    public void loadFromFile() throws IOException {
        synchronized (fileLock) {
            Path filePath = Paths.get(CONFIG_FILE_PATH);
            if (Files.exists(filePath)) {
                String data = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
                nodeIDs = Arrays.stream(data.split(","))
                        .map(String::trim) // Trim whitespace
                        .collect(Collectors.toList());
                System.out.println("Loaded node IDs from file: " + nodeIDs);
            } else {
                System.out.println("No node configuration found in file.");
            }
        }
    }




}

