package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;

public class Heartbeat extends Event {

    private final int messageType;
    private final ConnInfo connInfo;
    private final int freeSpace;

    public Heartbeat(int messageType, ConnInfo connInfo, int freeSpace) {
        this.messageType = messageType;
        this.connInfo = connInfo;
        this.freeSpace = freeSpace;
    }

    public ConnInfo getConnInfo() { return connInfo; }
    public int getFreeSpace() { return freeSpace; }

    @Override
    public int getType() {
        return messageType;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {
        writeString(dout, connInfo.getIP());
        dout.writeInt(connInfo.getPort());
        dout.writeInt(freeSpace);
    }
}
