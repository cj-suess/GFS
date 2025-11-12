package csx55.dfs.replication;

import csx55.dfs.transport.TCPConnection;
import csx55.dfs.transport.TCPSender;
import csx55.dfs.wireformats.ConnInfo;
import csx55.dfs.util.LogConfig;
import csx55.dfs.util.Protocol;
import csx55.dfs.wireformats.Event;
import csx55.dfs.wireformats.Register;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Controller implements Node {

    private final Logger log = Logger.getLogger(this.getClass().getName());
    private final Consumer<Exception> warning = e -> log.log(Level.WARNING, e.getMessage(), e);
    private Map<Integer, BiConsumer<Event, Socket>> events = new HashMap<>();

    private final int port;

    private final Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();
    private final Map<ConnInfo, TCPConnection> serverToConn = new ConcurrentHashMap<>();

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
                Protocol.REGISTER_REQUEST, this::handleRegisterRequest
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

    private void handleRegisterRequest(Event event, Socket socket) {
        log.info("Register request detected. Checking status...");
        TCPConnection conn = socketToConn.get(socket);
        TCPSender sender = conn.getSender();
        Register registerEvent = (Register) event;
        if (!serverToConn.containsKey(registerEvent.getChunkServerInfo())) {
            serverToConn.put(registerEvent.getChunkServerInfo(), conn);
            log.info(() -> registerEvent.getChunkServerInfo() + " was added to the list successfully!\n" + "\tCurrent number of chunk servers available: " + serverToConn.size());
        }
    }

    public static void main(String[] args) {
        LogConfig.init(Level.INFO);
        Controller controller = new Controller(Integer.parseInt(args[0]));
        new Thread(controller::startNode).start();
    }
}
