package csx55.dfs.wireformats;

import csx55.dfs.util.Protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.logging.*;

public class EventFactory {

    private final static Logger log = Logger.getLogger(EventFactory.class.getName());
    private final Consumer<Exception> warning = e -> log.log(Level.WARNING, e.getMessage(), e);
    private final byte[] data;
    private Map<Integer, EventReader> readers = new HashMap<>();

    public EventFactory(byte[] data) {
        this.data = data;
        startReaders();
    }

    public Event createEvent() {
        try(ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais)) {
            int messageType = dis.readInt();
            EventReader reader = readers.get(messageType);
            if(reader != null) {
                return reader.read(messageType, dis);
            }
        } catch(IOException e) { warning.accept(e); }
        return null;
    }

    private void startReaders() {
        readers = Map.of(
                Protocol.REGISTER_REQUEST, this::readRegisterRequest,
                Protocol.SERVER_REQUEST, this::readServerRequest,
                Protocol.SERVER_RESPONSE, this::readServerResponse,
                Protocol.HEARTBEAT, this::readHeartbeat,
                Protocol.STORE_REQUEST, this::readStoreRequest
        );
    }

    private Event readServerResponse(int messageType, DataInputStream dis) throws IOException {
        return new ServerResponse(messageType, dis);
    }

    private Event readServerRequest(int messageType, DataInputStream dis) throws IOException {
        return new ServerRequest(messageType);
    }

    private Event readStoreRequest(int messageType, DataInputStream dis) throws IOException {
        return new StoreRequest(messageType, readChunkData(dis), dis.readInt(), readString(dis), readString(dis), readServers(dis));
    }

    private Event readRegisterRequest(int messageType, DataInputStream dis) throws IOException {
        ConnInfo chunkServerInfo = readConnInfo(dis);
        return new Register(messageType, chunkServerInfo);
    }

    private Event readHeartbeat(int messageType, DataInputStream dis) throws IOException {
        return new Heartbeat(messageType, readConnInfo(dis), dis.readInt());
    }

    // utility methods
    private String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes);
    }

    private ConnInfo readConnInfo (DataInputStream dis) throws IOException {
        String ip = readString(dis);
        int port = dis.readInt();
        return new ConnInfo(ip, port);
    }

    private byte[] readChunkData(DataInputStream dis) throws IOException {
        int dataLen = dis.readInt();
        byte[] bytes = new byte[dataLen];
        dis.readFully(bytes);
        return bytes;
    }

    private Queue<ConnInfo> readServers(DataInputStream dis) throws IOException {
        int queueSize = dis.readInt();
        Queue<ConnInfo> queue = new LinkedList<>();
        for(int i = 0; i < queueSize; i++) {
            String ip = readString(dis);
            int port = dis.readInt();
            queue.add(new ConnInfo(ip, port));
        }
        return queue;
    }
}
