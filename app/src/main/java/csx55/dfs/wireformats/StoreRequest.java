package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Queue;

public class StoreRequest extends Event {

    private final int messageType;
    private final byte[] chunkData;
    private final int chunkIndex;
    private final String destination;
    private final String netID;
    private final Queue<ConnInfo> servers;

    public StoreRequest(int messageType, byte[] chunkData, int chunkIndex, String destination, String netID,  Queue<ConnInfo> servers) {
        this.messageType = messageType;
        this.chunkData = chunkData;
        this.chunkIndex = chunkIndex;
        this.destination = destination;
        this.netID = netID;
        this.servers =  servers;
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
    }

    public byte[] getChunkData() { return chunkData; }
    public int getChunkIndex() { return chunkIndex; }
    public String getDestination() { return destination; }
    public String getNetID() { return netID; }
    public Queue<ConnInfo> getServers() { return servers; }
}
