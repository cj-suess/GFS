package csx55.dfs.util;

import java.util.Objects;

public class ConnInfo implements Comparable<ConnInfo> {

    private final String ip;
    private final int port;

    public ConnInfo(String ip, int port){
        this.ip = ip;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof ConnInfo)) return false;
        ConnInfo nodeID = (ConnInfo) o;
        return port == nodeID.port && ip.equals(nodeID.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port);
    }

    @Override
    public int compareTo(ConnInfo o) {
        int ip = this.ip.compareTo(o.ip);
        if(ip != 0) {
            return ip;
        } return Integer.compare(this.port, o.port);
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }

    public String getIP(){
        return ip;
    }

    public int getPort() {
        return port;
    }

}

