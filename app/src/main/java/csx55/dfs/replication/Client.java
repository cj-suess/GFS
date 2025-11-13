package csx55.dfs.replication;

import csx55.dfs.transport.TCPConnection;
import csx55.dfs.util.Chunk;
import csx55.dfs.util.LogConfig;
import csx55.dfs.wireformats.ConnInfo;
import csx55.dfs.wireformats.Event;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client implements Node{

    private final Logger log = Logger.getLogger(this.getClass().getName());
    private final Consumer<Exception> warning = e -> log.log(Level.WARNING, e.getMessage(), e);

    private Map<Integer, BiConsumer<Event, Socket>> events = new HashMap<>();
    private Map<String, Consumer<String[]>> commands = new HashMap<>();

    private final ConnInfo controllerInfo;

    private final Map<Socket, TCPConnection> socketToConn = new ConcurrentHashMap<>();
    private Map<String, Map<Integer, Chunk>> files = new ConcurrentHashMap<>();

    public Client(String ip, int port) {
        this.controllerInfo = new ConnInfo(ip, port);
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
        events = Map.of(

        );
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
        String sourcePath = paths[0];
        String destPath =  paths[1];
        String fileName = Paths.get(sourcePath).getFileName().toString(); // get the filename from sourcePath
        System.out.println(fileName);
        Map<Integer, Chunk> chunks = new HashMap<>(); // map chunk index to chunk
        chunker(sourcePath, 65536, chunks);
        files.put(fileName, chunks);
    }

    // Path to test.txt --> /s/chopin/k/grad/camsuess/test/test.txt

    private void chunker(String filePath, int chunkSize, Map<Integer, Chunk> chunks) {
        File file = new File(filePath);
        try(FileInputStream fis = new FileInputStream(file);) {
            byte[] buffer = new byte[chunkSize];
            int len;
            int chunkIndex = 0;
            while((len = fis.read(buffer)) != -1) {
                String chunkName = filePath + "_"  + chunkIndex;
                try(FileOutputStream fos = new FileOutputStream(chunkName)){
                    fos.write(buffer, 0, len);
                    // create chunk
                    // get 3 random servers
                        // add servers to chunk's location list
                    // send chunk to first server
                    // add chunk to chunks map
                }
                chunkIndex++;
            }
        } catch (IOException e) {
            warning.accept(e);
        }
    }

    public static void main(String[] args) {
        LogConfig.init(Level.INFO);
        Client client = new Client(args[0], Integer.parseInt(args[1]));
        new Thread(client::startNode, "Node-" + client + "-Server").start();
        new Thread(client::readTerminal, "Node-" + client + "-Terminal").start();
    }
}
