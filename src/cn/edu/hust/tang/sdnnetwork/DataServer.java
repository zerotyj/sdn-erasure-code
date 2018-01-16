package cn.edu.hust.tang.sdnnetwork;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.edu.hust.tang.ec.JerasureLibrary;
import cn.edu.hust.tang.packets.AckPacket;
import cn.edu.hust.tang.packets.DataPacket;
import cn.edu.hust.tang.packets.LinkPacket;
import cn.edu.hust.tang.packets.Packet;
import cn.edu.hust.tang.packets.ServerInfo;

public class DataServer {
	
	private static final int PORT = 12345; 
	private static ServerSocket welSocket = null;
	private Socket conSocket = null;
	private DataOutputStream output = null;
	private DataInputStream input = null;
	private int factor; 
	private int size;	//数据包大小
	private Path dataPath;
	private FSDataInputStream ecInput;
	private FileSystem fileSystem;
	private long pos;
	private boolean isRemain;
	
	
	
	public DataServer(FileSystem fs) {
		fileSystem = fs;
	}
	
	public void readFile() throws IOException {
		
		try {
			ecInput.seek(pos);
			isRemain = true;
		} catch (IOException e) {
			isRemain = false;
		}
		System.out.println(String.format("open file %s, set pos at %d", dataPath.toString(), pos));
		int length = 0;
		byte[] data = new byte[size]; 
		byte[] actualData = new byte[size];
		while (true) {
			Packet pkt = Packet.readInput(input);
			if (pkt.getType() == Packet.READ_PACKET_TYPE) {
				if (!isRemain) {
					Arrays.fill(actualData, (byte)0x00);
					length = 0;
				} else {			
					int off = 0;
		            while(off < size) {
		                int haveRead = ecInput.read(data, off, size - off);
		                if(haveRead < 0) {
		                    isRemain = false;
		                    Arrays.fill(data, off, size, (byte)0x00);
		                    break;
		                }
		                off = off + haveRead;
		            }
		            length = off;

					//int haveRead = ecInput.read(data, 0, size);
					//if (haveRead < size) {
					//	isRemain = false;
					//	Arrays.fill(data, haveRead, size, (byte)0x00);
					//}
					//length = haveRead;
					dataProcess(factor, data, actualData);
				}
				DataPacket dataPkt = new DataPacket(size, actualData, length);
				Packet.writeOutput(output, dataPkt);
			} else if (pkt.getType() == Packet.CLOSE_PACKET_TYPE) {
				ecInput.close();
//				welSocket.close();
				break;
			} else {
				throw new IOException(String.format("wrong packet type: %d", pkt.getType()));
			}
		}
	}
		
	
	public void dataProcess(int factor, byte[] src, byte[] res) {
		if (factor != 1) {
			JerasureLibrary.INSTANCE.galois_w08_region_multiply(src, factor, src.length, res, 0);
		} else {
			System.arraycopy(src, 0, res, 0, src.length);
		}
//		System.out.printf("after process, the data length is %d\n", res.length);
//		CodecUtils.printMatrix(res);
	}
	
	
	public void initialize() throws IOException {
		try {
//			welSocket = new ServerSocket(PORT);
//			conSocket = welSocket.accept();
			input = new DataInputStream(conSocket.getInputStream());
			output = new DataOutputStream(conSocket.getOutputStream());
			Packet pkt = Packet.readInput(input);
			if (pkt.getType() == Packet.LINK_PACKET_TYPE) {
				System.out.println("receive link packet");
				LinkPacket linkPkt = (LinkPacket) pkt;
				ServerInfo info = linkPkt.getInfo();
//				String hostIP = InetAddress.getLocalHost().getHostAddress();
				dataPath = new Path(info.getPath()); 
				factor = info.getFactor();
				pos = info.getPos();
				size = linkPkt.getSize();
				System.out.println("dataPath: " + dataPath);
				ecInput = fileSystem.open(dataPath, size);
				AckPacket ackPkt = new AckPacket(true);
				Packet.writeOutput(output, ackPkt);
			} else {
				throw new IOException(String.format("wrong packet type: %d", pkt.getType()));
			}
		} catch (IOException e) {
			AckPacket ackPkt = new AckPacket(false);
			Packet.writeOutput(output, ackPkt);
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
		
			Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        while (true) {
	        	if (welSocket == null) {
	        		welSocket = new ServerSocket(PORT);
	        	}      	
	        	DataServer ds = new DataServer(fs);
	        	ds.conSocket = welSocket.accept();
	        	new ChildThread(ds).start();
	        }

	}
	
	

}

class ChildThread extends Thread {
	
	private DataServer ds;
	
	public ChildThread(DataServer ds) {
		this.ds = ds;
	}
	@Override
	public void run() {
		try {
			ds.initialize();
			ds.readFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
