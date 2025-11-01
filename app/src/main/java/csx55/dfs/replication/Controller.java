package csx55.dfs.replication;

import csx55.dfs.transport.TCPConnection;
import csx55.dfs.util.LogConfig;
import csx55.dfs.wireformats.Event;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Controller implements Node {

    private final Logger log = Logger.getLogger(this.getClass().getName());
    private final Map<Integer, BiConsumer<Event, Socket>> events = new HashMap<>();

    private final int port;

    private final Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();

    public Controller(int port) {
        this.port = port;
    }

    @Override
    public void onEvent(Event event, Socket socket) {

    }

    @Override
    public void startNode() {
        try(ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("Discovery node is up and running. Listening on port: " + port);
            while(true) {
                Socket clientSocket = serverSocket.accept();
                InetSocketAddress client = (InetSocketAddress) clientSocket.getRemoteSocketAddress();
                log.info("New connection from: " + client.getAddress() + ":" + client.getPort());
                TCPConnection conn = new TCPConnection(clientSocket, this);
                conn.startReceiverThread();
                socketToConn.put(clientSocket, conn);
            }
        } catch(IOException e) {
            log.log(Level.WARNING, "Exception while starting discovery node...", e);
        }
    }

    public static void main(String[] args) {
        LogConfig.init(Level.INFO);
        Controller controller = new Controller(Integer.parseInt(args[0]));
        new Thread(controller::startNode).start();
    }
}
