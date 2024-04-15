package org.example.dbnode.Broadcast;

import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Model.NodeInfo;
import org.example.dbnode.Model.Request;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Log4j2
public class Broadcaster {

    private static final int NODES_NO = 4;
    private final static int NODE_ID = NodeInfo.getInstance().getNodeId();
    public static void broadcast(Request request) {
        request.addHeader("X-Broadcast", "true");
        HttpEntity<Map<String, Object>> requestEntity = request.getRequestEntity();
        HttpMethod method = request.getMethod();
        RestTemplate restTemplate = new RestTemplate();

        for (int i = 1; i <= NODES_NO; i++) {
            if (i != NODE_ID) {
                log.info("Broadcasting to node " + i + "...");
                String url = request.getUrl().replaceAll("NODE_ID", String.valueOf(i));
                restTemplate.exchange(url, method, requestEntity, String.class);
            }
        }
    }
}
