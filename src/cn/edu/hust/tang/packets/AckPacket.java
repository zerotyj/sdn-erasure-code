package cn.edu.hust.tang.packets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AckPacket extends Packet {

	private boolean linkResult;
	
	public AckPacket() {
		super(ACK_PACKET_TYPE);
	}
	
	public AckPacket(boolean res) {
		super(ACK_PACKET_TYPE);
		linkResult = res;
	}
	
	public boolean getResult() {
		return linkResult;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(linkResult);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		linkResult = in.readBoolean();
	}

}
