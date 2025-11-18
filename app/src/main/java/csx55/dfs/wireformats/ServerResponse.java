package csx55.dfs.wireformats;

import csx55.dfs.util.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class ServerResponse extends Event {

    private final int messageType;
    private final Queue<ConnInfo> servers;

    public ServerResponse(int messageType, Queue<ConnInfo> servers) {
        this.messageType = messageType;
        this.servers = servers;
    }

    public ServerResponse(int messageType, DataInputStream dis) throws IOException {
        this.messageType = messageType;
        int serversLen  = dis.readInt();
        servers = new LinkedList<>();
        for (int i = 0; i < serversLen; i++) {
            String ip = readString(dis);
            int port = dis.readInt();
            servers.add(new ConnInfo(ip, port));
        }
    }

    @Override
    public int getType() {
        return Protocol.SERVER_RESPONSE;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {
        dout.writeInt(servers.size());
        for (ConnInfo server : servers) {
            writeString(dout, server.getIP());
            dout.writeInt(server.getPort());
        }
    }

    public Queue<ConnInfo> getServers() {
        return servers;
    }

    private String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes);
    }

}
