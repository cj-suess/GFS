package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;

public class RetrieveResponse extends Event {

    private final int messageType;
    private final ConnInfo connInfo;
    private final byte[] chunkData;

    public RetrieveResponse(int messageType, ConnInfo connInfo) {
        this.messageType = messageType;
        this.connInfo = connInfo;
        this.chunkData = null;
    }

    public RetrieveResponse(int messageType, byte[] chunkData) {
        this.messageType = messageType;
        this.connInfo = null;
        this.chunkData = chunkData;
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
}
