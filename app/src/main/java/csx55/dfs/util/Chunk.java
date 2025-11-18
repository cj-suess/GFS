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

    private final Converter converter;
    private final byte[] data;
    private final int chunkIndex;
    private final String checksum;
    private final List<ConnInfo> locations = new ArrayList<>(); // list of where chunk replications are stored
    private final Map<Integer, String> slices = new HashMap<>(); // mapping slice index to checksum hex

    public Chunk(byte[] data, int chunkIndex) {
        this.converter = Converter.getConverter();
        this.data = data;
        this.chunkIndex = chunkIndex;
        this.checksum = createChecksum();
        createSlices();
    }

    private void createSlices() {
        try{
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            int sliceSize = 8192;
            int sliceIndex = 1;
            for(int i = 0; i < data.length; i+=sliceSize) {
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

    private String createChecksum() {
        String checksum = null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            int size =  data.length;
            byte[] hash = md.digest(data);
            checksum = converter.convertBytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            warning.accept(e);
        }
        return checksum;
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
