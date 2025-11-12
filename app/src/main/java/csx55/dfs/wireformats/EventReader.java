package csx55.dfs.wireformats;

import java.io.DataInputStream;
import java.io.IOException;

public interface EventReader {
    Event read(int messageType, DataInputStream dis) throws IOException;
}
