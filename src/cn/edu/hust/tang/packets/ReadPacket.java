package cn.edu.hust.tang.packets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReadPacket extends Packet {

	public ReadPacket() {
		super(READ_PACKET_TYPE);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {

	}

	@Override
	public void readFields(DataInput in) throws IOException {

	}

}
