package org.example.dbnode.Affinity;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class AffinityBalancer {
    private final TreeMap<Long, Integer> ring;
    private final int numberOfReplicas;
    private final MessageDigest md;
    private static final int CLUSTER_SIZE = 4;

    private static class InstanceHolder {
        public static AffinityBalancer instance = new AffinityBalancer(300);
    }

    public static AffinityBalancer getInstance() {
        return InstanceHolder.instance;
    }

    public AffinityBalancer(int numberOfReplicas){
        this.ring = new TreeMap<>();
        this.numberOfReplicas = numberOfReplicas;
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        for(int i = 1; i <= CLUSTER_SIZE; i++) {
            addNode(i);
        }
    }

    public void addNode(int nodeId) {
        for (int i = 0; i < numberOfReplicas; i++) {
            long hash = generateHash(("node"+nodeId)+ i);
            ring.put(hash, nodeId);
        }
    }

    public void removeNode(int nodeId) {
        for (int i = 0; i < numberOfReplicas; i++) {
            long hash = generateHash(("node"+nodeId)+ i);
            ring.remove(hash);
        }
    }

    public Integer getNodeWithAffinityId(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        long hash = generateHash(key);
        if (!ring.containsKey(hash)) {
            SortedMap<Long, Integer> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    private long generateHash(String key) {
        md.reset();
        md.update(key.getBytes());
        byte[] digest = md.digest();
        long hash = ((long) (digest[3] & 0xFF) << 24) |
                ((long) (digest[2] & 0xFF) << 16) |
                ((long) (digest[1] & 0xFF) << 8) |
                ((long) (digest[0] & 0xFF));
        return hash;
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        AffinityBalancer ch = new AffinityBalancer(300);

        Map<Integer, Integer> nodes = new HashMap<>();
        nodes.put(1, 0);
        nodes.put(2, 0);
        nodes.put(3, 0);
        nodes.put(4, 0);

        for(int i = 0; i < 10000; i++) {
            String key = UUID.randomUUID().toString();
            int nodeId = ch.getNodeWithAffinityId(key);
            nodes.put(nodeId,nodes.getOrDefault(nodeId, 0)+1);
        }

        for(Integer server : nodes.keySet()) {
            System.out.println(server + ": " + nodes.get(server));
        }
    }
}


