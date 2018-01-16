package cn.edu.hust.tang.sdnnetwork;

import java.io.DataOutputStream;
import java.io.IOException;

import cn.edu.hust.tang.packets.Packet;
import cn.edu.hust.tang.packets.ReadPacket;

public class RequestDataThread extends Thread {
	
	private volatile boolean closed = false;
	private DataOutputStream output = null;
	private int readRequestNum;
	
	public RequestDataThread(DataOutputStream output, int num) {
		this.output = output;
		this.readRequestNum = num;
		
	}
	
	public void setClosed(boolean closed) {
		this.closed = closed;
	}
	
	public boolean getClosed() {
		return closed;
	}
	
	@Override
	public void run() {
		for(int i = 0; i < readRequestNum; i++) {
			ReadPacket readPkt = new ReadPacket();
			try {
				Packet.writeOutput(output, readPkt);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("request thread is end");
	}
}
