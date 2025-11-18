package csx55.dfs.wireformats;

import csx55.dfs.util.Chunk;
import csx55.dfs.util.ChunkMetadata;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class Heartbeat extends Event {

    private final int messageType;
    private final ConnInfo connInfo;
    private final long freeSpace;
    private final int totalNumChunks;
    private final List<ChunkMetadata> chunks;

    public Heartbeat(int messageType, ConnInfo connInfo, long freeSpace, int totalNumChunks, List<ChunkMetadata> chunks) {
        this.messageType = messageType;
        this.connInfo = connInfo;
        this.freeSpace = freeSpace;
        this.totalNumChunks = totalNumChunks;
        this.chunks = chunks;
    }

    public ConnInfo getConnInfo() { return connInfo; }
    public long getFreeSpace() { return freeSpace; }
    public int getTotalNumChunks() { return totalNumChunks; }
    public List<ChunkMetadata> getChunks() { return chunks; }

    @Override
    public int getType() {
        return messageType;
    }

    @Override
    void marshalData(DataOutputStream dout) throws IOException {
        writeString(dout, connInfo.getIP());
        dout.writeInt(connInfo.getPort());
        dout.writeLong(freeSpace);
        dout.writeInt(totalNumChunks);
        dout.writeInt(chunks.size());
        for(ChunkMetadata chunk : chunks) {
            writeString(dout, chunk.getFileName());
            dout.writeInt(chunk.getChunkIndex());
            writeString(dout, chunk.getChecksum());
            dout.writeInt(chunk.getSize());
        }
    }
}
