package cn.edu.hust.tang.packets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataPacket extends Packet {

	private int size;
	private int length = 0; //数据实际长度
	private byte[] data;
	
	public DataPacket() {
		super(DATA_PACKET_TYPE);
	}
	
	public DataPacket(int size, byte[] data, int length) {
		super(DATA_PACKET_TYPE);
		if (data.length <= size) {
			this.size = size;
			this.length = length; 
			this.data = new byte[size];
			System.arraycopy(data, 0, this.data, 0, data.length);
		} else {
			System.err.println("DataPacket initialize error");
		}
	}
	
	public int getLength() {
		return length;
	}
	
	public byte[] getData() {
		return data;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		out.writeInt(length);
			out.write(data);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		size = in.readInt();
		length = in.readInt();
			data = new byte[size];
			in.readFully(data);
	}

}
