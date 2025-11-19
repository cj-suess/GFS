package csx55.dfs.replication;

import csx55.dfs.transport.TCPConnection;
import csx55.dfs.util.ChunkMetadata;
import csx55.dfs.util.Converter;
import csx55.dfs.wireformats.*;
import csx55.dfs.util.LogConfig;
import csx55.dfs.util.Protocol;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ChunkServer implements Node {

    private final Object lock = new Object();

    private Logger log = Logger.getLogger(this.getClass().getName());
    private final Consumer<Exception> warning = e -> log.log(Level.WARNING, e.getMessage(), e);
    private Map<Integer, BiConsumer<Event, Socket>> events = new HashMap<>();

    private final ConnInfo controllerInfo;
    private TCPConnection controllerConn;

    private ConnInfo myConnInfo;
    private final Converter converter = Converter.getConverter();

    private final Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();

    private final List<ChunkMetadata> allChunks = new ArrayList<>();
    private final List<ChunkMetadata> newChunkMetadata = new ArrayList<>();

    private long freeSpace;

    public ChunkServer(String ip, int port) {
        this.controllerInfo = new ConnInfo(ip, port);
        this.freeSpace = 1073741824L;
        startEvents();
    }

    @Override
    public void onEvent(Event event, Socket socket) {
        if(event != null) {
            BiConsumer<Event, Socket> biConsumer = events.get(event.getType());
            biConsumer.accept(event, socket);
        } else{
            warning.accept(new Exception("event is null"));
        }
    }

    @Override
    public void startEvents() {
        events = Map.of(
            Protocol.STORE_REQUEST, this::handleStoreRequest,
            Protocol.RETRIEVE_REQUEST, this::handleRetrieveRequest
        );
    }

    @Override
    public void startNode() {
        try(ServerSocket serverSocket = new ServerSocket(0)) {
            myConnInfo = new ConnInfo(InetAddress.getLocalHost().getHostAddress(), serverSocket.getLocalPort());
            log = Logger.getLogger(ChunkServer.class.getName() + "[" + myConnInfo + "]");
            register();
            new Thread(this::startHeartbeat, "Heartbeat-" + myConnInfo).start();
            while(true) {
                Socket clientSocket = serverSocket.accept();
                InetSocketAddress client = (InetSocketAddress) clientSocket.getRemoteSocketAddress();
                log.info("New connection from: " + client.getAddress() + ":" + client.getPort());
                TCPConnection conn = new TCPConnection(clientSocket, this);
                conn.startReceiverThread();
                socketToConn.put(clientSocket, conn);
            }
        } catch(IOException e) {
            warning.accept(e);
        }
    }

    private void handleRetrieveRequest(Event event, Socket socket) {
        RetrieveRequest retrieveRequest = (RetrieveRequest) event;
        String destination = retrieveRequest.getFileName();
        int chunkIndex =  retrieveRequest.getChunkIndex();
        String path = String.format("/tmp/%s/chunk_server/%s_chunk%d","camsuess", destination,chunkIndex);
        File chunkFile = new File(path);
        byte[] chunkData = null;
        try(FileInputStream fis = new FileInputStream(chunkFile)) {
            int fileSize = (int) chunkFile.length();
            chunkData = new byte[fileSize];
            int bytesRead = fis.read(chunkData);
        } catch (Exception e) {
            warning.accept(e);
        }
        RetrieveResponse retrieveResponse = new RetrieveResponse(Protocol.RETRIEVE_RESPONSE, chunkData);
        TCPConnection conn = socketToConn.get(socket);
        try{
            conn.sender.sendData(retrieveResponse.getBytes());
        } catch(IOException e) {
            warning.accept(e);
        }
    }

    private void handleStoreRequest(Event event, Socket socket) {
        try{
            StoreRequest storeRequest = (StoreRequest) event;
            byte[] chunkData =  storeRequest.getChunkData();
            int chunkIndex =  storeRequest.getChunkIndex();
            String destination =  storeRequest.getDestination();
            String netID =   storeRequest.getNetID();
            Queue<ConnInfo> servers = storeRequest.getServers();
            String path = String.format("/tmp/%s/chunk_server/%s_chunk%d",netID,destination,chunkIndex);
            File chunkFile = new File(path);
            chunkFile.getParentFile().mkdirs();
            long oldSize = chunkFile.exists() ? chunkFile.length() : 0; // for overwriting to keep freeSpace accurate
            try(FileOutputStream fos = new FileOutputStream(chunkFile)) {
                fos.write(chunkData);
            }
            freeSpace += oldSize;
            freeSpace -= chunkData.length;
            String checksum = computeChecksum(chunkData);
            ChunkMetadata metadata = new ChunkMetadata(destination, chunkIndex, checksum, chunkData.length);
            synchronized (lock) {

                allChunks.removeIf(m -> m.getFileName().equals(destination) && m.getChunkIndex() == chunkIndex);
                newChunkMetadata.removeIf(m -> m.getFileName().equals(destination) && m.getChunkIndex() == chunkIndex);

                allChunks.add(metadata);
                newChunkMetadata.add(metadata);
            }
            printStoredChunks();
            log.info(() -> "Space remaining --> " + freeSpace);
            if(!servers.isEmpty()){
                forward(storeRequest, servers);
            } else {
                log.info(() -> "Last chunk has been stored successfully....");
            }
        } catch (IOException e) {
            warning.accept(e);
        }
    }

    private void forward(StoreRequest oldRequest, Queue<ConnInfo> servers) {
        try{
            ConnInfo nextServer = servers.poll();
            log.info(() -> "Forwarding chunk to next server: " + nextServer);
            StoreRequest newRequest = new StoreRequest(Protocol.STORE_REQUEST, oldRequest.getChunkData(), oldRequest.getChunkIndex(), oldRequest.getDestination(), oldRequest.getNetID(), servers);
            Socket socket = new Socket(nextServer.getIP(), nextServer.getPort());
            TCPConnection conn = new TCPConnection(socket, this);
            conn.sender.sendData(newRequest.getBytes());
            socket.close();
        } catch(Exception e) {
            warning.accept(e);
        }
    }

    private void register() {
        try{
            Socket socket = new Socket(controllerInfo.getIP(), controllerInfo.getPort());
            controllerConn = new TCPConnection(socket, this);
            Register registerMessage = new Register(Protocol.REGISTER_REQUEST, getMyConnInfo());
            controllerConn.startReceiverThread();
            controllerConn.sender.sendData(registerMessage.getBytes());
        } catch(IOException e) {
            warning.accept(e);
        }
    }

    private void sendMinorHeartbeat() {
        List<ChunkMetadata> sendNew;
        synchronized (lock) {
            sendNew = new ArrayList<>(newChunkMetadata);
        }
        Heartbeat heartbeat = new Heartbeat(Protocol.HEARTBEAT, getMyConnInfo(), getFreeSpace(), allChunks.size(), sendNew);
        try{
            if(controllerConn != null) {
                controllerConn.sender.sendData(heartbeat.getBytes());
                //log.info("Sent minor heartbeat with " + sendNew.size() + " new chunks");
            }
        } catch(IOException e) {
            warning.accept(e);
        }
        synchronized (lock) {
            newChunkMetadata.clear();
        }
    }

    private void sendMajorHeartbeat() {
        List<ChunkMetadata> sendAll;
        synchronized (lock) {
            sendAll = new ArrayList<>(allChunks);
        }
        Heartbeat heartbeat = new Heartbeat(Protocol.HEARTBEAT, getMyConnInfo(), getFreeSpace(), allChunks.size(), sendAll);
        try{
            if(controllerConn != null) {
                controllerConn.sender.sendData(heartbeat.getBytes());
                //log.info("Sent major heartbeat with " + sendAll.size() + " total chunks");
            }
        } catch(IOException e) {
            warning.accept(e);
        }
    }

    private void startHeartbeat() {
        int counter = 0;
        while(true) {
            try {
                if(counter % 4 == 0) {
                    sendMajorHeartbeat();
                } else {
                    sendMinorHeartbeat();
                }
                counter++;
                Thread.sleep(15000);
            } catch(InterruptedException e) {
                warning.accept(e);
            }
        }
    }

    private String computeChecksum(byte[] data) {
        String checksum = null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] hash = md.digest(data);
            checksum = converter.convertBytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            warning.accept(e);
        }
        return checksum;
    }

    private void printStoredChunks() {
        List<ChunkMetadata> chunksToPrint;
        synchronized (lock) {
            chunksToPrint = new ArrayList<>(allChunks);
        }
        for(ChunkMetadata chunk : chunksToPrint) {
            log.info("Chunk: " + chunk.getFileName() + " index=" + chunk.getChunkIndex() + " checksum=" + chunk.getChecksum());
        }
    }

    public ConnInfo getMyConnInfo() { return myConnInfo; }

    public long getFreeSpace() { return freeSpace; }

    public static void main(String[] args) {
        LogConfig.init(Level.WARNING);
        ChunkServer server = new ChunkServer(args[0], Integer.parseInt(args[1]));
        new Thread(server::startNode).start();
    }
}
