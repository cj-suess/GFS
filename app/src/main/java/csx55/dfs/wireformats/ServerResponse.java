package csx55.dfs.wireformats;

import csx55.dfs.util.Protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class ServerResponse extends Event implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Queue<ConnInfo> servers;

    public ServerResponse(Queue<ConnInfo> servers) {
        this.servers = servers;
    }

    @Override
    public int getType() {
        return Protocol.SERVER_RESPONSE;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {

    }

    public Queue<ConnInfo> getServers() {
        return servers;
    }

}
