package cn.edu.hust.tang.packets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import restapi.SimpleBuildTree;

public class LinkPacket extends Packet {
	
	private ServerInfo info;
	private int size;
//	private ArrayList<ServerInfo> info;
	private SimpleBuildTree tree;
	
	public LinkPacket() {
		super(LINK_PACKET_TYPE);
	}
	
	public LinkPacket(SimpleBuildTree tree, ServerInfo info, int size) {
		super(LINK_PACKET_TYPE);
		this.tree = tree;
		this.info = info;
		this.size = size;
	}
	
	public SimpleBuildTree getTree() {
		return tree;
	}
	
	public ServerInfo getInfo() {
		return info;
	}

	public int getSize() {
		return size;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		ObjectOutputStream output =new ObjectOutputStream((OutputStream) out);
		output.writeInt(size);
		output.writeObject(info);
		output.writeObject(tree);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ObjectInputStream input =new ObjectInputStream((InputStream) in);
		try {
			size = input.readInt();
			info = (ServerInfo) input.readObject();
			tree = (SimpleBuildTree) input.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}

