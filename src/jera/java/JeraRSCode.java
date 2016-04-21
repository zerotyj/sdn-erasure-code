/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jera.java;

import cn.edu.hust.xie.localityencode.main.ec.ErasureCode;
import cn.edu.hust.xie.localityencode.main.ec.TooManyErasedLocations;

/**
 *
 * @author yvanhom
 */
public class JeraRSCode implements ErasureCode {
    private int stripeSize;
    private int paritySize;
    private long pointer = 0;
    
    public native long jniInitialize(int stripe, int parity);
    public native void jniEncode(long pointer, byte[][] datas, byte[][] parities, int size);
    public native void jniFindLocations(long pointer, int[] erased, int[] result);
    public native void jniDecode(long pointer, byte[][] datas, byte[][] parities, int size, int[] erased);
    public native void jniRelease(long pointer);
    
    static {
        System.loadLibrary("JeraRSCode"); 
    }
    
    @Override
    public void initialize(int s, int p) {
        stripeSize = s;
        paritySize = p;
        if(pointer != 0) {
            jniRelease(pointer);
            pointer = 0;
        }
        pointer = jniInitialize(s, p);
    }

    @Override
    public int stripeCount() {
        return stripeSize;
    }

    @Override
    public int parityCount() {
        return paritySize;
    }

    @Override
    public void encode(byte[][] datas, byte[][] parities, int len) {
        jniEncode(pointer, datas, parities, len);
    }

    @Override
    public int[] locationsToReadForDecode(int[] erased) throws TooManyErasedLocations{
        if(erased.length > paritySize) {
            throw new TooManyErasedLocations(String.format("%d is larger than %d", erased.length, paritySize));
        }
        int[] result = new int[stripeSize];
        
        jniFindLocations(pointer, erased, result);
        
        return result;
    }

    @Override
    public void decode(byte[][] datas, byte[][] parities, int len, int[] erased) {
        jniDecode(pointer, datas, parities, len, erased);
    }
    
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if(pointer != 0) {
            jniRelease(pointer);
            pointer = 0;
        }
    }
}
