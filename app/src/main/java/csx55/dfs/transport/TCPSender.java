package csx55.dfs.transport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class TCPSender {

    private final DataOutputStream dout;

    public TCPSender(Socket socket) throws IOException {
        dout = new DataOutputStream(socket.getOutputStream());
    }

    public synchronized void sendData(byte[] dataToSend) throws IOException {
        int dataLength = dataToSend.length;
        dout.writeInt(dataLength);
        dout.write(dataToSend, 0, dataLength);
        dout.flush();
    }
}
