package cn.edu.hust.tang.packets;

import java.io.Serializable;


@SuppressWarnings("serial")
public class ServerInfo implements Serializable {
	
	private String ipAddr;
	private String blockPath;	//服务器上的文件路径
	private long pos;	//偏移量
	private int factor;	
	private ServerInfo[] childInfo = null;
	
	public ServerInfo() {
		childInfo = null;		
	}
	
	public ServerInfo(String ip, String path, long pos, int factor) {
		this.ipAddr = ip;
		this.blockPath = path;
		this.pos = pos;
		this.factor = factor;
	}
    
	public void setInfo(String ip, String path, long pos, int factor) {
		ipAddr = ip;
		blockPath = path;
		this.pos = pos;
		this.factor = factor;
	}
	
	public void setChildInfo(ServerInfo[] info) {
		childInfo = info;
	}
	
	public ServerInfo[] getChildInfo() {
		return childInfo;
	}
	
	
	public String getIP() {
		return ipAddr;
	}
	
	
    public String getPath() {
    	return blockPath;
    }
      
    
    public long getPos() {
    	return pos;
    }
    
    public int getFactor() {
    	return factor;
    }
}
