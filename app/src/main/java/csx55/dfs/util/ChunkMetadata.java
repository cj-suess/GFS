package csx55.dfs.util;

import java.util.Objects;

public class ChunkMetadata {

    private final String fileName;
    private final int chunkIndex;
    private final String checksum;
    private final int size;
    private final long timestamp;

    public ChunkMetadata(String fileName, int chunkIndex, String checksum, int size) {
        this.fileName = fileName;
        this.chunkIndex = chunkIndex;
        this.checksum = checksum;
        this.size = size;
        this.timestamp = System.currentTimeMillis();
    }

    public String getFileName() {
        return fileName;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public String getChecksum() {
        return checksum;
    }

    public int getSize() {
        return size;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChunkMetadata)) return false;
        ChunkMetadata other = (ChunkMetadata) o;
        return chunkIndex  == other.chunkIndex && fileName.equals(other.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, chunkIndex);
    }

    @Override
    public String toString() {
        return fileName + ":" + chunkIndex + ":" + checksum;
    }
}
