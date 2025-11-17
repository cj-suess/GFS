package csx55.dfs.replication;

import csx55.dfs.transport.TCPConnection;
import csx55.dfs.util.Chunk;
import csx55.dfs.util.LogConfig;
import csx55.dfs.util.Protocol;
import csx55.dfs.wireformats.*;

import java.io.*;
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

public class Client implements Node{

    private final Logger log = Logger.getLogger(this.getClass().getName());
    private final Consumer<Exception> warning = e -> log.log(Level.WARNING, e.getMessage(), e);

    private final Map<Integer, BiConsumer<Event, Socket>> events = new HashMap<>();
    private final Map<String, Consumer<String[]>> commands = new HashMap<>();

    private final ConnInfo controllerInfo;
    private final String netID;

    private final Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, Chunk>> files = new ConcurrentHashMap<>();

    public Client(String ip, int port) {
        this.controllerInfo = new ConnInfo(ip, port);
        netID = "camsuess";
        startEvents();
        startCommands();
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

    }

    @Override
    public void startNode() {
        try(ServerSocket serverSocket = new ServerSocket(0)) {
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

    private void readTerminal() {
        try(Scanner scanner = new Scanner(System.in)) {
            while(true) {
                String command = scanner.nextLine();
                String[] parsedCommand = command.split("\\s+");
                if(parsedCommand.length == 0 || parsedCommand[0].isEmpty()) { continue; }
                Consumer<String[]> action = commands.get(parsedCommand[0]);
                if(action == null) { log.info(() -> "Please enter a valid command..."); }
                String[] args =  Arrays.copyOfRange(parsedCommand, 1, parsedCommand.length);
                assert action != null;
                action.accept(args);
            }
        } catch(NullPointerException e) {
            warning.accept(e);
        }
    }

    private void startCommands() {
        commands.put("upload", this::upload);
    }

    private void upload(String[] paths) {
        String source = paths[0];
        String destination = paths[1];

        Map<Integer, Chunk> chunks = chunker(source); // break into chunks

        for (Map.Entry<Integer, Chunk> entry : chunks.entrySet()) {
            int chunkIndex = entry.getKey();
            Chunk chunk = entry.getValue();
            log.info(() -> "Uploading chunk: " + chunkIndex);
            Queue<ConnInfo> servers = requestServers(); // get servers from controller
            for (ConnInfo server : servers) {
                chunk.addLocation(server);
            }
            sendChunk(chunk, servers, destination); // send to first server and let handle the forwarding
        }
        log.info(() -> "Upload completed...");
        files.put(source, chunks); // add completed file to map
        printChunkLocations(source); // print ip:port for each replicate
    }

    private Map<Integer, Chunk> chunker(String source) {
        Map<Integer, Chunk> chunks = new LinkedHashMap<>();
        File file = new File(source);
        try(FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[65536];
            int len;
            int chunkIndex = 0;
            while ((len = fis.read(buffer)) != -1) {
                byte[] data = Arrays.copyOf(buffer, len);
                Chunk chunk = new Chunk(data, chunkIndex);
                chunks.put(chunkIndex, chunk);
                chunkIndex++;
            }
            log.info(() -> "File has been successfully chunked...");
        } catch(IOException e) {
            warning.accept(e);
        }
        return chunks;
    }

    private Queue<ConnInfo> requestServers() { // going to not use events for this part since getting server information back to upload is annoying
        Queue<ConnInfo> servers = new LinkedList<>();
        try {
            Socket socket = new Socket(controllerInfo.getIP(), controllerInfo.getPort());
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            // send request
            ServerRequest request = new ServerRequest(Protocol.SERVER_REQUEST);
            oos.writeObject(request);
            oos.flush();
            // read response
            ServerResponse response = (ServerResponse) ois.readObject();
            socket.close();
            return response.getServers();
        } catch(IOException | ClassNotFoundException e) {
            warning.accept(e);
        }
        return servers;
    }

    private void sendChunk(Chunk chunk, Queue<ConnInfo> servers, String destination) {
        try{
            ConnInfo firstServer = servers.poll();
            log.info("Sending chunk to first server in list --> " + firstServer);
            StoreRequest storeRequest = new StoreRequest(Protocol.STORE_REQUEST, chunk.getData(), chunk.getChunkIndex(), destination, netID, servers);
            Socket socket = new Socket(firstServer.getIP(), firstServer.getPort());
            TCPConnection  conn = new TCPConnection(socket, this);
            conn.startReceiverThread();
            conn.sender.sendData(storeRequest.getBytes());
        } catch(IOException e) {
            warning.accept(e);
        }
    }

    private void printChunkLocations(String source) {
        Map<Integer, Chunk> chunks = files.get(source);
        for(Chunk chunk : chunks.values()) {
            for(ConnInfo server : chunk.getLocations()){
                System.out.println(server);
            }
        }
    }

    public static void main(String[] args) {
        LogConfig.init(Level.INFO);
        Client client = new Client(args[0], Integer.parseInt(args[1]));
        new Thread(client::startNode, "Node-" + client + "-Server").start();
        new Thread(client::readTerminal, "Node-" + client + "-Terminal").start();
    }
}
