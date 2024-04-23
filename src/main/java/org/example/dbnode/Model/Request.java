package org.example.dbnode.Model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
public class Request {
    private String url;
    private HttpMethod method;
    private final Map<String, Object> params;
    private JsonNode body; // Change the type of body to JsonNode
    private final HttpHeaders headers;

    public Request() {
        params = new HashMap<>();
        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
    }

    public Request addParam(String key, Object value){
        params.put(key,value);
        return this;
    }

    public Request addHeader(String key, String value){
        headers.add(key,value);
        return this;
    }

    public Request setUrl(String url) {
        this.url = url;
        return this;
    }

    public Request setBody(JsonNode body) { // Change the parameter type to JsonNode
        this.body = body;
        return this;
    }

    public Request setMethod(HttpMethod method) {
        this.method = method;
        return this;
    }

    public Request addAuthHeaders(String username, String password){
        headers.set("username", username);
        headers.set("password", password);
        return this;
    }

    public HttpEntity<Object> getRequestEntity(){
        if (body != null) {
            // If a body is given, include the parameters in the URL
            UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url);
            params.forEach(builder::queryParam);
            url = builder.toUriString();
        }
        return new HttpEntity<>(body != null ? body : params, headers);
    }
}