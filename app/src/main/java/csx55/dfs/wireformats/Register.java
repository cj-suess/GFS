package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;

public class Register extends Event {

    private final int messageType;
    private final ConnInfo chunkServerInfo;

    public Register(int messageType, ConnInfo chunkServerInfo) {
        this.messageType = messageType;
        this.chunkServerInfo = chunkServerInfo;
    }

    public ConnInfo getChunkServerInfo() {
        return chunkServerInfo;
    }

    @Override
    public int getType() {
        return messageType;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {
        writeString(dout, chunkServerInfo.getIP());
        dout.writeInt(chunkServerInfo.getPort());
    }
}

