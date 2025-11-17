package csx55.dfs.util;

import csx55.dfs.replication.ChunkServer;
import csx55.dfs.wireformats.ConnInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Chunk {

    private byte[] data;
    private int chunkIndex;
    private List<ConnInfo> locations = new ArrayList<>(); // list of where chunk replications are stored
    private Map<Integer, String> slices = new HashMap<>(); // mapping slice index to checksum hex

    public Chunk(byte[] data, int chunkIndex) {
        this.data = data;
        this.chunkIndex = chunkIndex;
        createSlices();
    }

    public byte[] getData() {
        return data;
    }
    public int getChunkIndex() {
        return chunkIndex;
    }

    public List<ConnInfo> getLocations() {
        return locations;
    }

    public void addLocation(ConnInfo location) {
        locations.add(location);
    }

    public Map<Integer, String> getSlices() {
        return slices;
    }

    private void createSlices() {
        
    }
}
