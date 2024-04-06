package org.example.dbnode.Controller;

import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Model.NodeInfo;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
@Log4j2
@RestController
@RequestMapping("/api/node/info")
public class NodeInfoController {

    private final NodeInfo nodeInfo  = NodeInfo.getInstance();

    @PostMapping
    public ResponseEntity<String> setNodeInfo(int nodeId, String nodeIP, boolean isActive) {
        nodeInfo.setNodeId(nodeId);
        nodeInfo.setNodeIP(nodeIP);
        nodeInfo.setActive(isActive);
        String response = "Node \""+nodeId+"\" info has been set successfully";
        log.info(response);
        return ResponseEntity.ok(response);
    }

}
