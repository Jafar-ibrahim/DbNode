package org.example.dbnode.Model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.*;

import java.util.Map;
@NoArgsConstructor
@Getter
public class Document {

    private ObjectNode content;

    public Document(ObjectNode content) {
        this.content = content;
    }

    public void setContent(ObjectNode content) {
        this.content = content;
    }

    public String getId() {
        return content.get("_id").asText();
    }
    public void setId(String id) {
        content.put("_id", id);
    }
    public Long getVersion() {
        return content.get("_version").asLong();
    }
    public void setVersion(Long version) {
        content.put("_version", version);
    }
    public boolean isReplicated() {
        return content.get("replicated").asBoolean();
    }
    public void setReplicated(boolean replicated) {
        content.put("replicated", replicated);
    }


}
