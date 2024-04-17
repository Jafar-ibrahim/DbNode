package org.example.dbnode.Affinity;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Exception.RedirectionException;
import org.example.dbnode.Model.Request;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
@Log4j2
public class RedirectionService {
    public static ResponseEntity<String> redirect(Request request,int nodeId) {
        request.addHeader("X-Broadcast", "false");
        HttpEntity<Object> requestEntity = request.getRequestEntity();
        HttpMethod method = request.getMethod();
        RestTemplate restTemplate = new RestTemplate();
        String url = request.getUrl().replaceAll("NODE_ID", String.valueOf(nodeId));

        ResponseEntity<String> response;
        try {
            response = restTemplate.exchange(url, method, requestEntity, String.class);
        } catch (Exception e) {
            String[] parts = e.getMessage().split(":");
            int errorCode = Integer.parseInt(parts[0].trim());
            String errorMessage = parts[1].trim();
            log.error("Response from affinity node: " + e.getMessage());
            throw new RedirectionException(errorMessage, errorCode);
        }

        if (response.getStatusCode().is4xxClientError() || response.getStatusCode().is5xxServerError()) {
            String errorMessage = response.getBody();
            int errorCode = response.getStatusCode().value();
            System.out.println("error code: " + errorCode);
            throw new RedirectionException(errorMessage, errorCode);
        }
        return response;
    }
}
