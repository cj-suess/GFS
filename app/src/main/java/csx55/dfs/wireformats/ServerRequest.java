package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class ServerRequest extends Event implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int messageType;

    public ServerRequest(int messageType) {
        this.messageType = messageType;
    }

    @Override
    public int getType() {
        return messageType;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {

    }
}
