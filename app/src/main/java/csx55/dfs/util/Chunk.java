package csx55.dfs.util;

import csx55.dfs.replication.ChunkServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Chunk {

    List<ChunkServer> locations = new ArrayList<>(); // list of where chunk replications are stored
    Map<Integer, String> slices = new HashMap<>(); // mapping slice index to checksum hex

    public Chunk() {
        // createSlices();
    }
}
