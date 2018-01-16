package restapi;

import java.io.Serializable;

@SuppressWarnings("serial")
public class SimpleBuildTree implements Serializable{
    private String hostIp;
    private SimpleBuildTree[] childIps;

    public String getHostIp() {
        return hostIp;
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }

    public SimpleBuildTree[] getChildIps() {
        return childIps;
    }

    public void setChildIps(SimpleBuildTree[] childIps) {
        this.childIps = childIps;
    }
}

