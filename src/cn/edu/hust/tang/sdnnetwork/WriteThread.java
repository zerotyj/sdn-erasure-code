package cn.edu.hust.tang.sdnnetwork;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cn.edu.hust.tang.packets.DataPacket;
import cn.edu.hust.tang.packets.Packet;
import cn.edu.hust.tang.packets.ReadPacket;

public class WriteThread extends Thread {
	
	private volatile boolean closed = false;
	private Buffer[] buffer = new Buffer[2];
	private DataInputStream input = null;
	private DataOutputStream output = null;
	private long blockOffset;
	private long actualBlockSize;
	private RequestDataThread request = null;
	private int readRequestNum;
	
	
	public WriteThread(Buffer[] buffer, DataInputStream input
			, DataOutputStream output, long blockOffset, long actualBlockSize) {
		this.buffer = buffer;
		this.input = input;
		this.output = output;
		this.blockOffset = blockOffset;
		this.actualBlockSize = actualBlockSize;
		this.readRequestNum = (int) (actualBlockSize / buffer[0].getSize());
		if (actualBlockSize % buffer[0].getSize() != 0) {
			readRequestNum++;
		}
	}
	
	public void setClosed(boolean closed) {
		this.closed = closed;
	}
	
//	把blockOffest和actualBlockSize传给写线程，用来作为线程结束的标志
	
	@SuppressWarnings("deprecation")
	@Override
	public void run() {
		request = new RequestDataThread(output, readRequestNum);
		request.start();
		while (!closed) {
			try {
				if (blockOffset >= actualBlockSize) {
					buffer[Buffer.getWriteBufferIndex()].writeBuffer(null);
					System.out.println("write thread is end");
					break;
				}
				if (buffer[Buffer.getWriteBufferIndex()].isEmpty()) {
//					ReadPacket readPkt = new ReadPacket();
//					Packet.writeOutput(output, readPkt);
					Packet pkt = Packet.readInput(input);
					if (pkt.getType() == Packet.DATA_PACKET_TYPE) {
						DataPacket dataPkt = (DataPacket) pkt;
						if (dataPkt.getLength() == 0) {
//							Buffer.setReachEnd(true);
							buffer[Buffer.getWriteBufferIndex()].writeBuffer(null);
							break;
						} else {
							long length = Math.min(dataPkt.getLength(), actualBlockSize - blockOffset);
							byte[] data = new byte[(int) length];
							blockOffset += length;
							System.arraycopy(dataPkt.getData(), 0, data, 0, (int) length);
							buffer[Buffer.getWriteBufferIndex()].writeBuffer(data);
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (request.isAlive()) {
			System.out.println("request is alive");
    		request.setClosed(true);
    		System.out.println("request status: " + request.getClosed());
    		try {
    			System.out.println("ready to enter request");
    			request.join();
    			System.out.println("leave request");
    		} catch (InterruptedException e) {
    			System.out.println("request thread error:" + e.getMessage());
    		}
    		//request.stop();
    	}
	}
}
