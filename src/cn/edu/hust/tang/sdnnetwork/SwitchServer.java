package cn.edu.hust.tang.sdnnetwork;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import cn.edu.hust.tang.ec.JerasureLibrary;
import cn.edu.hust.tang.packets.*;
import restapi.SimpleBuildTree;


public class SwitchServer {
	
	private static final int PORT = 12345;
	private int serNum;
	private int size;
	private Socket cliSocket = null;
	private ServerSocket welSocket = null;
	private Socket[] serSocket;
	private String[] ipAddr;
	private DataInputStream[] inputFromServer;
	private DataOutputStream[] outputToServer;
	private DataInputStream inputFromClient = null;
	private DataOutputStream outputToclient = null;
	
	
	public void initialize() throws IOException {
		try {
			welSocket = new ServerSocket(PORT);
			cliSocket = welSocket.accept();
			inputFromClient = new DataInputStream(cliSocket.getInputStream());
			outputToclient = new DataOutputStream(cliSocket.getOutputStream());

			Packet pkt = Packet.readInput(inputFromClient);
			if (pkt.getType() == Packet.LINK_PACKET_TYPE) {
				LinkPacket linkPkt = (LinkPacket) pkt;
				ServerInfo info = linkPkt.getInfo();
				SimpleBuildTree tree = linkPkt.getTree();
				
				serNum = tree.getChildIps().length;
				size = linkPkt.getSize();
				serSocket = new Socket[serNum];
				ipAddr = new String[serNum];
				for (int i = 0; i < serNum; i++) {
					ipAddr[i] = tree.getChildIps()[i].getHostIp();
				}
				inputFromServer = new DataInputStream[serNum];
				outputToServer = new DataOutputStream[serNum];
				
				for (int i=0; i < serNum; i++) {
					
					System.out.println(ipAddr[i]);
					serSocket[i] = new Socket(ipAddr[i], PORT);
					inputFromServer[i] = new DataInputStream(serSocket[i].getInputStream());
					outputToServer[i] = new DataOutputStream(serSocket[i].getOutputStream());
					
					Packet.writeOutput(outputToServer[i], new LinkPacket(tree.getChildIps()[i], info.getChildInfo()[i], size));
					Packet pkt1 = Packet.readInput(inputFromServer[i]);
					if (pkt1.getType() == Packet.ACK_PACKET_TYPE) {
						AckPacket ackPkt = (AckPacket) pkt1;
						if (!ackPkt.getResult()) {
							System.out.printf("Server %d link error", i);
							throw new IOException(String.format("Server %d link error", i));
						}
					} else {
						throw new IOException(String.format("wrong packet type: %d", pkt1.getType()));
					}
				}
				AckPacket ackPkt = new AckPacket(true);
				Packet.writeOutput(outputToclient, ackPkt);
							
			} else {
				throw new IOException(String.format("wrong packet type: %d", pkt.getType()));
			}
		} catch (IOException e) {
			AckPacket ackPkt = new AckPacket(false);
			Packet.writeOutput(outputToclient, ackPkt);
			e.printStackTrace();
		}
	}
	
	
	public void dataProcess() {
		try {
			while (true) {					
				Packet pkt = Packet.readInput(inputFromClient);
				if (pkt.getType() == Packet.READ_PACKET_TYPE) {			
					for(int i=0; i < serNum; i++) {
						Packet.writeOutput(outputToServer[i], (ReadPacket) pkt);
					}
				} else if (pkt.getType() == Packet.CLOSE_PACKET_TYPE) {
					for(int i=0; i < serNum; i++) {
						Packet.writeOutput(outputToServer[i], (ClosePacket) pkt);
					}
					cliSocket.close();
					welSocket.close();
					break;
				} else {
					throw new IOException(String.format("wrong packet type: %d", pkt.getType()));
				}
				int init = 0;
				int length = 0;
				byte[] res = new byte[size];
				for(int i=0; i < serNum; i++) {
					Packet pkt1 = Packet.readInput(inputFromServer[i]); //交换机在从服务器读数据时发生空指针异常
					if (pkt1.getType() == Packet.DATA_PACKET_TYPE) {
						DataPacket dataPkt = (DataPacket) pkt1;
						byte[] src = dataPkt.getData();
						if (init == 0) {
							System.arraycopy(src, 0, res, 0, src.length);
							length = dataPkt.getLength();
							init = 1;
						} else {
							length = Math.max(length, dataPkt.getLength());
							JerasureLibrary.INSTANCE.galois_region_xor(src, res, size);
						}
					} else {
						throw new IOException(String.format("wrong packet type: %d", pkt.getType()));
					}
				}
//				CodecUtils.printMatrix(res);
				DataPacket dataPkt = new DataPacket(size, res, length);
				Packet.writeOutput(outputToclient, dataPkt);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	public static void main(String[] args) {
		SwitchServer sw = new SwitchServer();
		try {
			while (true) {
				sw.initialize();
				sw.dataProcess();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

