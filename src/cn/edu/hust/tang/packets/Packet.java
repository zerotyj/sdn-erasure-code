package cn.edu.hust.tang.packets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Packet {
    // 定义数据包的类型
	public static final byte LINK_PACKET_TYPE = 0x00;//连接请求数据包
	public static final byte ACK_PACKET_TYPE = 0x01;//连接请求回复数据包
	public static final byte READ_PACKET_TYPE = 0x02;//读取数据请求数据包
	public static final byte DATA_PACKET_TYPE = 0x03;//数据回复数据包
	public static final byte CLOSE_PACKET_TYPE = 0x04;//关闭连接数据包

    private final byte type;

    public Packet(byte t) {
        this.type = t;
    }

    public byte getType() {
        return type;
    }

    // 将数据包写入输出流
    public abstract void write(DataOutput out) throws IOException;
    // 从输入流中读取数据包
    public abstract void readFields(DataInput in) throws IOException;

    // 先写类型，再写数据包
    public static void writeOutput(DataOutput out, Packet p) throws IOException {
        out.writeByte(p.type);	
        //如果是断开连接数据包,则不用写数据
        //if(p.type != CLOSE_PACKET_TYPE)
        p.write(out);
    }

    // 先读类型，再读数据包
    public static Packet readInput(DataInput in) throws IOException {
        byte type = in.readByte();	
        Packet ret;
        switch(type) {
        	case LINK_PACKET_TYPE:
        		ret = new LinkPacket();
        		break;
        	case ACK_PACKET_TYPE:
        		ret = new AckPacket();
        		break;
        	case READ_PACKET_TYPE:
        		ret = new ReadPacket();
        		break;
        	case DATA_PACKET_TYPE:
        		ret = new DataPacket();
        		break;
        	case CLOSE_PACKET_TYPE:
        		ret = new ClosePacket();
        		break;
        	default:
        		throw new IOException(String.format("Unknown packet type %d",  type));
        }
        //如果是断开连接数据包,则不用读数据
        //if(type != CLOSE_PACKET_TYPE)
        ret.readFields(in);
        return ret;
    }
}

