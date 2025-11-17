package csx55.dfs.util;


import csx55.dfs.wireformats.ConnInfo;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Chunk {

    private final Logger log = Logger.getLogger(this.getClass().getName());
    private final Consumer<Exception> warning = e -> log.log(Level.WARNING, e.getMessage(), e);

    private byte[] data;
    private int chunkIndex;
    private Converter converter;
    private List<ConnInfo> locations = new ArrayList<>(); // list of where chunk replications are stored
    private Map<Integer, String> slices = new HashMap<>(); // mapping slice index to checksum hex

    public Chunk(byte[] data, int chunkIndex) {
        this.data = data;
        this.chunkIndex = chunkIndex;
        this.converter = Converter.getConverter();
        createSlices();
    }

    private void createSlices() {
        try{
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            int sliceSize = 8192;
            int sliceIndex = 0;
            for(int i = 0; i < sliceSize; i+=sliceSize) {
                byte[] slice = Arrays.copyOfRange(data, i, Math.min(i + sliceSize, data.length));
                byte[] hash =  md.digest(slice);
                String checksum = converter.convertBytesToHex(hash);
                slices.put(sliceIndex, checksum);
                sliceIndex++;
            }
        } catch(NoSuchAlgorithmException e) {
            warning.accept(e);
        }
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
}
