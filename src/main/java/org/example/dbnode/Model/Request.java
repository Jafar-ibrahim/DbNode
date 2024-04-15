package org.example.dbnode.Model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

import java.util.HashMap;
import java.util.Map;
@Setter
@Getter
public class Request {
    private String url;
    private HttpMethod method;
    private final Map<String, Object> params;
    private final HttpHeaders headers;

    public Request() {
        params = new HashMap<String, Object>();
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

    public Request setMethod(HttpMethod method) {
        this.method = method;
        return this;
    }
    public Request addAuthHeaders(String username, String password){
        headers.set("username", username);
        headers.set("password", password);
        return this;
    }

    public HttpEntity<Map<String, Object>> getRequestEntity(){
        return new HttpEntity<>(params, headers);
    }
}
