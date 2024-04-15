package org.example.dbnode.Affinity;

import org.example.dbnode.Model.Request;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

public class RedirectionService {

    public static ResponseEntity<String> redirect(Request request,int nodeId) {
        request.addHeader("X-Broadcast", "false");
        HttpEntity<Map<String, Object>> requestEntity = request.getRequestEntity();
        HttpMethod method = request.getMethod();
        RestTemplate restTemplate = new RestTemplate();
        String url = request.getUrl().replaceAll("NODE_ID", String.valueOf(nodeId));
        return restTemplate.exchange(url, method, requestEntity, String.class);
    }
}
