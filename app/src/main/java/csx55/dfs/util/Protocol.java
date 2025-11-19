package csx55.dfs.util;

public interface Protocol {
    int REGISTER_REQUEST = 1;
    int HEARTBEAT = 2;
    int SERVER_REQUEST = 3;
    int SERVER_RESPONSE = 4;
    int STORE_REQUEST = 5;
    int RETRIEVE_REQUEST = 6;
    int RETRIEVE_RESPONSE = 7;
    int FIX_REQUEST = 8;
    int FIX_RESPONSE = 9;
}
