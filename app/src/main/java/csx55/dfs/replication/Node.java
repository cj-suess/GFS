package csx55.dfs.replication;

import csx55.dfs.wireformats.Event;
import java.net.Socket;

public interface Node {
    void onEvent(Event event, Socket socket);

    void startNode();

    void startEvents();
}
