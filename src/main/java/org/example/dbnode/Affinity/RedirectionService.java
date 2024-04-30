package org.example.dbnode.Affinity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Exception.RedirectionException;
import org.example.dbnode.Model.Request;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Map;
@Log4j2
public class RedirectionService {
    public static ResponseEntity<String> redirect(Request request,int nodeId) {
        request.addHeader("isBroadcast", "false");
        HttpEntity<Object> requestEntity = request.getRequestEntity();
        HttpMethod method = request.getMethod();
        RestTemplate restTemplate = new RestTemplate();
        String url = request.getUrl().replaceAll("NODE_ID", String.valueOf(nodeId));

        ResponseEntity<String> response;
        try {
            response = restTemplate.exchange(url, method, requestEntity, String.class);
        } catch (Exception e) {
            log.error("Response from affinity node: " + e.getMessage());
            String errorMessage;
            int errorCode;
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode errorJson = mapper.readTree(e.getMessage());
                errorCode = errorJson.get("status").asInt();
                errorMessage = errorJson.get("error").asText();
            } catch (NullPointerException | JsonProcessingException jsonException) {
                String[] parts = e.getMessage().split(":");
                errorCode = Integer.parseInt(parts[0].trim());
                errorMessage = parts[1].trim();
            }
            throw new RedirectionException(errorMessage, errorCode);
        }

        if (response.getStatusCode().is4xxClientError() || response.getStatusCode().is5xxServerError()) {
            String errorMessage = response.getBody();
            int errorCode = response.getStatusCode().value();
            throw new RedirectionException(errorMessage, errorCode);
        }
        return response;
    }
}
