package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;

public class RetrieveRequest extends Event {

    private final int messageType;
    private final String fileName;
    private final int chunkIndex;

    public RetrieveRequest(int messageType, String fileName, int chunkIndex) {
        this.messageType = messageType;
        this.fileName = fileName;
        this.chunkIndex = chunkIndex;
    }

    @Override
    public int getType() {
        return messageType;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {
        writeString(dout, fileName);
        dout.writeInt(chunkIndex);
    }

    public String  getFileName() {
        return fileName;
    }
    public int getChunkIndex() {
        return chunkIndex;
    }
}
