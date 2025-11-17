package csx55.dfs.replication;

import csx55.dfs.transport.TCPConnection;
import csx55.dfs.util.ChunkServerMetadata;
import csx55.dfs.wireformats.*;
import csx55.dfs.util.LogConfig;
import csx55.dfs.util.Protocol;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Controller implements Node {

    private final Object lock = new Object();

    private final Logger log = Logger.getLogger(this.getClass().getName());
    private final Consumer<Exception> warning = e -> log.log(Level.WARNING, e.getMessage(), e);
    private Map<Integer, BiConsumer<Event, Socket>> events = new HashMap<>();

    private final int port;

    private final Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();
    private final Map<ConnInfo, TCPConnection> serverToConn = new ConcurrentHashMap<>();

    private final PriorityQueue<ChunkServerMetadata> serversBySpace = new PriorityQueue<>();
    private final Map<ConnInfo, ChunkServerMetadata> chunkServers = new ConcurrentHashMap<>();

    public Controller(int port) {
        this.port = port;
        startEvents();
    }

    @Override
    public void onEvent(Event event, Socket socket) {
        if(event != null) {
            BiConsumer<Event, Socket> biConsumer = events.get(event.getType());
            biConsumer.accept(event, socket);
        } else {
            warning.accept(new Exception("event is null"));
        }
    }

    @Override
    public void startEvents() {
        events = Map.of(
                Protocol.REGISTER_REQUEST, this::handleRegisterRequest,
                Protocol.HEARTBEAT, this::handleHeartbeat,
                Protocol.SERVER_REQUEST, this::handleServerRequest
        );
    }

    @Override
    public void startNode() {
        try(ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("Controller node launched. Listening on port: " + port);
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

    private void handleServerRequest(Event event, Socket socket) {
        Queue<ConnInfo> servers = selectServers();
        ServerResponse response = new ServerResponse(servers);
        try{
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(response);
            oos.flush();
        } catch (IOException e) {
            warning.accept(e);
        }
    }

    private Queue<ConnInfo> selectServers() {
        Queue<ConnInfo> servers = new LinkedList<>();
        synchronized (lock) {
            List<ChunkServerMetadata> temp = new ArrayList<>();
            for(int i = 0; i < 3; i++){
                ChunkServerMetadata server = serversBySpace.poll();
                assert server != null;
                servers.add(server.getConnInfo());
                temp.add(server);
            }
            serversBySpace.addAll(temp);
        }
        return servers;
    }

    private void handleRegisterRequest(Event event, Socket socket) {
        log.info("Register request detected. Checking status...");
        TCPConnection conn = socketToConn.get(socket);
        Register registerEvent = (Register) event;
        ConnInfo chunkServerInfo = registerEvent.getChunkServerInfo();
        if (!serverToConn.containsKey(chunkServerInfo)) {
            serverToConn.put(chunkServerInfo, conn);
            ChunkServerMetadata metadata = new ChunkServerMetadata(chunkServerInfo);
            chunkServers.put(chunkServerInfo, metadata);
            synchronized (lock) {
                serversBySpace.add(metadata);
            }
            log.info(() -> registerEvent.getChunkServerInfo() + " was added to the list successfully!\n" + "\tCurrent number of chunk servers available: " + serverToConn.size());
        }
    }

    private void handleHeartbeat(Event event, Socket socket) {
        Heartbeat heartbeat = (Heartbeat) event;
        ConnInfo chunkServerInfo = heartbeat.getConnInfo();
        int freeSpace = heartbeat.getFreeSpace();
        ChunkServerMetadata metaData = chunkServers.get(chunkServerInfo);
        if(metaData != null) {
            updateSpace(metaData, freeSpace);
            log.info(() -> "Updated free space for " + chunkServerInfo + " --> " + freeSpace);
        }
    }

    private void updateSpace(ChunkServerMetadata metaData, int freeSpace) {
        synchronized (lock) {
            serversBySpace.remove(metaData);
            metaData.setFreeSpace(freeSpace);
            serversBySpace.add(metaData);
        }
    }

    public static void main(String[] args) {
        LogConfig.init(Level.INFO);
        Controller controller = new Controller(Integer.parseInt(args[0]));
        new Thread(controller::startNode).start();
    }
}
