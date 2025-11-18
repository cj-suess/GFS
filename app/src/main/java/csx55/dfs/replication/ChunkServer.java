package csx55.dfs.replication;

import csx55.dfs.transport.TCPConnection;
import csx55.dfs.wireformats.ConnInfo;
import csx55.dfs.util.LogConfig;
import csx55.dfs.util.Protocol;
import csx55.dfs.wireformats.Event;
import csx55.dfs.wireformats.Register;
import csx55.dfs.wireformats.StoreRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
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
    private ConnInfo myConnInfo;

    private final Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();

    private int freeSpace;

    public ChunkServer(String ip, int port) {
        this.controllerInfo = new ConnInfo(ip, port);
        this.freeSpace = 0;
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
            Protocol.STORE_REQUEST, this::handleStoreRequest
        );
    }

    @Override
    public void startNode() {
        try(ServerSocket serverSocket = new ServerSocket(0)) {
            myConnInfo = new ConnInfo(InetAddress.getLocalHost().getHostAddress(), serverSocket.getLocalPort());
            log = Logger.getLogger(ChunkServer.class.getName() + "[" + myConnInfo + "]");
            register();
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
            try(FileOutputStream fos = new FileOutputStream(chunkFile)) {
                fos.write(chunkData);
            }
            freeSpace -= chunkData.length;
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
            TCPConnection controllerConn = new TCPConnection(socket, this);
            Register registerMessage = new Register(Protocol.REGISTER_REQUEST, getMyConnInfo());
            controllerConn.startReceiverThread();
            controllerConn.sender.sendData(registerMessage.getBytes());
        } catch(IOException e) {
            warning.accept(e);
        }
    }

    public ConnInfo getMyConnInfo() { return myConnInfo; }

    public int getFreeSpace() { return freeSpace; }

    public static void main(String[] args) {
        LogConfig.init(Level.INFO);
        ChunkServer server = new ChunkServer(args[0], Integer.parseInt(args[1]));
        new Thread(server::startNode).start();
    }
}
