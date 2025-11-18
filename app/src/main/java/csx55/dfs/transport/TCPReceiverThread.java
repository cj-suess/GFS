package csx55.dfs.transport;

import csx55.dfs.replication.Node;
import csx55.dfs.wireformats.Event;
import csx55.dfs.wireformats.EventFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Logger;

public class TCPReceiverThread implements Runnable {

    private final Socket socket;
    private final DataInputStream din;
    private final Node node;

    private final Logger LOG = Logger.getLogger(TCPReceiverThread.class.getName());

    public TCPReceiverThread(Socket socket, Node node) throws IOException {
        this.socket = socket;
        this.node = node;
        din = new DataInputStream(socket.getInputStream());
    }

    public void run() {
        int dataLength;
        while(socket != null) {
            try {
                dataLength = din.readInt();
                byte[] data = new byte[dataLength];
                din.readFully(data, 0, dataLength);
                // pass data to EventFactory
                EventFactory ef = new EventFactory(data);
                Event decodedEvent = ef.createEvent();
                node.onEvent(decodedEvent, socket);
            } catch(SocketException | EOFException e) {
                //LOG.info("Connection closed by peer..." + socket.getRemoteSocketAddress());
                break;
            } catch(IOException ioe) {
                LOG.warning("IO exception caught reading data..." + ioe.getLocalizedMessage());
                break;
            }
        }

    }
}
