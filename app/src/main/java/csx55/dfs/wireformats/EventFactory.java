package csx55.dfs.wireformats;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.logging.*;

public class EventFactory {

    private final static Logger log = Logger.getLogger(EventFactory.class.getName());
    private final Consumer<Exception> warning = e -> log.log(Level.WARNING, e.getMessage(), e);
    private final byte[] data;

    public EventFactory(byte[] data) {
        this.data = data;
    }

    public Event createEvent() {
        try(ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais)) {

            int messageType = dis.readInt();

            log.info(() -> "New event received --> " + messageType);

            switch (messageType) {
                default:
                    warning.accept(null);
                    break;
            }

        } catch(IOException e) {
            warning.accept(e);
        }
        return null;
    }
}
