package org.example.dbnode.Model;

import lombok.Getter;
import lombok.Setter;
import lombok.Singular;

@Setter
@Getter
public class NodeInfo {
    private int nodeId;
    private String nodeIP;
    private boolean isActive;
    private static NodeInfo instance;

    public static NodeInfo getInstance() {
        if (instance == null) {
            instance = new NodeInfo();
        }
        return instance;
    }
}
