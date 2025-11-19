package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class RetrieveResponse extends Event {

    private final int messageType;
    private final ConnInfo connInfo;
    private final byte[] chunkData;
    private Map<Integer, String> checksums;

    public RetrieveResponse(int messageType, ConnInfo connInfo) {
        this.messageType = messageType;
        this.connInfo = connInfo;
        this.chunkData = null;
        this.checksums = null;
    }

    public RetrieveResponse(int messageType, byte[] chunkData, Map<Integer, String> checksums) {
        this.messageType = messageType;
        this.connInfo = null;
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
        } else if(connInfo != null) {
            dout.writeByte(1);
            writeString(dout, connInfo.getIP());
            dout.writeInt(connInfo.getPort());
        }
    }

    public ConnInfo getConnInfo() {
        return connInfo;
    }
    public byte[] getChunkData() {
        return chunkData;
    }
    public Map<Integer, String> getChecksums() { return checksums; }
}
