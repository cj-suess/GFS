package csx55.dfs.wireformats;

import java.io.DataOutputStream;
import java.io.IOException;

public class StoreRequest extends Event {

    private final int messageType;

    public StoreRequest(int messageType) {
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
