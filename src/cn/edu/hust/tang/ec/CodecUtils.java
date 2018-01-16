package cn.edu.hust.tang.ec;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

public class CodecUtils {
	
	public static void printMatrix(byte[] m) {
		for (int i=0; i < m.length; i++) {
			System.out.printf("%02x ", m[i]);
		}
		System.out.println();
	}
	
	public static Pointer[] toPointerArray(byte[][] array) {
	    Pointer[] ptrArray = new Pointer[array.length];
	    for (int i = 0; i < array.length; ++i) {
	      ptrArray[i] = new Memory(array[i].length);
	      ptrArray[i].write(0, array[i], 0, array[i].length);
	    }
	    return ptrArray;
	}
	
	public static void toByteArray(Pointer[] ptrArray, byte[][] array) {
		for (int i = 0; i < array.length; ++i) {
			byte[] arr = ptrArray[i].getByteArray(0, array[i].length);
		    System.arraycopy(arr, 0, array[i], 0, array[i].length);
		}
	}
}
