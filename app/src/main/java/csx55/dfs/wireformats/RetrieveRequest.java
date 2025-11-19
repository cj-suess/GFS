package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;

public class RetrieveRequest extends Event {

    private final int messageType;
    private final String fileName;
    private final int chunkIndex;
    private final ConnInfo connInfo;

    public RetrieveRequest(int messageType, String fileName, int chunkIndex) {
        this.messageType = messageType;
        this.fileName = fileName;
        this.chunkIndex = chunkIndex;
        this.connInfo = null;
    }

    public RetrieveRequest(int messageType, String fileName, int chunkIndex, ConnInfo connInfo) {
        this.messageType = messageType;
        this.fileName = fileName;
        this.chunkIndex = chunkIndex;
        this.connInfo = connInfo;
    }

    @Override
    public int getType() {
        return messageType;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {
        if(connInfo == null) {
            dout.writeByte(0);
            writeString(dout, fileName);
            dout.writeInt(chunkIndex);
        } else {
            dout.writeByte(1);
            writeString(dout, fileName);
            dout.writeInt(chunkIndex);
            writeString(dout, connInfo.getIP());
            dout.writeInt(connInfo.getPort());
        }
    }

    public String  getFileName() {
        return fileName;
    }
    public int getChunkIndex() {
        return chunkIndex;
    }

    public ConnInfo getConnInfo() {
        return connInfo;
    }
}
