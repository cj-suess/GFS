package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;

public class StoreRequest extends Event {

    private final int messageType;
    private final byte[] chunkData;
    private final int chunkIndex;
    private final String destination;
    private final String netID;
    private final Queue<ConnInfo> servers;
    private final Map<Integer, String> checksums;

    public StoreRequest(int messageType, byte[] chunkData, int chunkIndex, String destination, String netID,  Queue<ConnInfo> servers,  Map<Integer, String> checksums) {
        this.messageType = messageType;
        this.chunkData = chunkData;
        this.chunkIndex = chunkIndex;
        this.destination = destination;
        this.netID = netID;
        this.servers =  servers;
        this.checksums = checksums;
    }

    @Override
    public int getType() {
        return messageType;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {
        dout.writeInt(chunkData.length);
        dout.write(chunkData);
        dout.writeInt(chunkIndex);
        writeString(dout, destination);
        writeString(dout, netID);
        dout.writeInt(servers.size());
        for (ConnInfo server : servers) {
            writeString(dout, server.getIP());
            dout.writeInt(server.getPort());
        }
        dout.writeInt(checksums.size());
        for (Map.Entry<Integer, String> entry : checksums.entrySet()) {
            dout.writeInt(entry.getKey());
            writeString(dout, entry.getValue());
        }
    }

    public byte[] getChunkData() { return chunkData; }
    public int getChunkIndex() { return chunkIndex; }
    public String getDestination() { return destination; }
    public String getNetID() { return netID; }
    public Queue<ConnInfo> getServers() { return servers; }
    public Map<Integer, String> getChecksums() { return checksums; }
}
