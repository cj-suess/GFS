package csx55.dfs.util;

import csx55.dfs.replication.ChunkServer;
import csx55.dfs.wireformats.ConnInfo;

import java.util.Objects;

public class ChunkServerMetadata implements Comparable<ChunkServerMetadata> {

    private final ConnInfo connInfo;
    private long freeSpace;
    private long lastHeartBeat;

    public ChunkServerMetadata(ConnInfo connInfo) {
        this.connInfo = connInfo;
        this.freeSpace = 0;
        this.lastHeartBeat = System.currentTimeMillis();
    }

    public ConnInfo getConnInfo() { return connInfo; }

    public long getFreeSpace() { return freeSpace; }

    public long getLastHeartBeat() { return lastHeartBeat; }

    public void setFreeSpace(long freeSpace) {
        this.freeSpace = freeSpace;
        this.lastHeartBeat = System.currentTimeMillis();
    }

    @Override
    public int compareTo(ChunkServerMetadata o) {
        long spaceCompare = Long.compare(o.freeSpace, this.freeSpace); // flip to keep higher space at top?
        if(spaceCompare != 0) return (int) spaceCompare;
        return this.connInfo.compareTo(o.connInfo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChunkServerMetadata)) return false;
        ChunkServerMetadata other = (ChunkServerMetadata) o;
        return Objects.equals(connInfo, other.connInfo);
    }

    @Override
    public int hashCode() { return Objects.hash(connInfo); }
}
