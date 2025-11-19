package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class RetrieveResponse extends Event {

    private final int messageType;
    private List<ConnInfo> servers;
    private final byte[] chunkData;
    private final Map<Integer, String> checksums;

    public RetrieveResponse(int messageType, List<ConnInfo> servers) {
        this.messageType = messageType;
        this.servers = servers;
        this.chunkData = null;
        this.checksums = null;
    }

    public RetrieveResponse(int messageType, byte[] chunkData, Map<Integer, String> checksums) {
        this.messageType = messageType;
        this.servers = null;
        this.chunkData = chunkData;
        this.checksums = checksums;
    }

    @Override
    public int getType() {
        return messageType;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {
        if(chunkData != null) {
            dout.writeByte(0);
            dout.writeInt(chunkData.length);
            dout.write(chunkData);
            dout.writeInt(checksums.size());
            for (Map.Entry<Integer, String> entry : checksums.entrySet()) {
                dout.writeInt(entry.getKey());
                writeString(dout, entry.getValue());
            }
        } else if(servers != null) {
            dout.writeByte(1);
            dout.writeInt(servers.size());
            for (ConnInfo server : servers) {
                writeString(dout, server.getIP());
                dout.writeInt(server.getPort());
            }
        }
    }

    public List<ConnInfo> getServers() {
        return servers;
    }
    public byte[] getChunkData() {
        return chunkData;
    }
    public Map<Integer, String> getChecksums() { return checksums; }
}
