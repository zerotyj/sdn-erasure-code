package cn.edu.hust.tang.sdnnetwork;

public class Buffer {
	private static int readBufferIndex = 0;
	private static int writeBufferIndex = 0;
	private static boolean reachEnd = false; //	标志是否到达块尾
	private int size; //缓冲区大小
	private int actualSize; //实际存储数据量
	private int offest;	//有效数据位置
	private byte[] data;
	
	public Buffer(int size) {
		this.size = size;
		actualSize = 0;
		offest = 0;
		data = new byte[size];
		reachEnd = false;
		readBufferIndex = 0;
		writeBufferIndex = 0;
	}
	
	public synchronized boolean isEmpty() {
		return actualSize == 0 ? true : false;
	}
	
	public int getSize() {
		return size;
	}
	
//	从缓冲区中读取最多len个字节到数组b中偏移为off的位置
	public synchronized int readBuffer(byte[] b, int off, int len) {
//		如果已经到达块尾且缓冲区没有数据，则返回-1
		if (reachEnd && actualSize == 0) {
			return -1;
		} else if (actualSize == 0) {
			return 0;
		} 
		int ret = Math.min(actualSize, len);
		System.arraycopy(data, offest, b, off, ret);
		actualSize -= ret;
		if (actualSize == 0) {
			offest = 0;
			readBufferIndex = (readBufferIndex + 1) % 2;
		} else {				
			offest += ret;
		}
		return ret;
	}
	
//	缓冲区为空时才能写入数据
	public synchronized void writeBuffer(byte[] b) {
		if (b == null) {
			reachEnd = true;
		} else if (b.length <= size) {
			System.arraycopy(b, 0, data, 0, b.length);
			actualSize = b.length;
			offest = 0;
			writeBufferIndex = (writeBufferIndex + 1) % 2;
		}
	}

	public static int getReadBufferIndex() {
		return readBufferIndex;
	}

	public static int getWriteBufferIndex() {
		return writeBufferIndex;
	}
	
//	public synchronized static void setReachEnd(boolean b) {
//		reachEnd = b;
//	}

}
