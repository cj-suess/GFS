package csx55.dfs.replication;

import csx55.dfs.wireformats.Event;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ChunkServer implements Node {

    private final Logger log = Logger.getLogger(this.getClass().getName());
    private final Consumer<Exception> warning = e -> log.log(Level.WARNING, e.getMessage(), e);
    private Map<Integer, BiConsumer<Event, Socket>> events = new HashMap<>();

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

    }

    public static void main(String[] args) {

    }
}
