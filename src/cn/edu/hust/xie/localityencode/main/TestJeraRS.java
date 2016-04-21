/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hust.xie.localityencode.main;

import cn.edu.hust.xie.localityencode.main.ec.ErasureCode;
import cn.edu.hust.xie.localityencode.main.ec.TooManyErasedLocations;
import java.util.Arrays;
import jera.java.JeraRSCode;

/**
 *
 * @author yvanhom
 */
public class TestJeraRS {

    private static void printData(String msg, byte[][] d, byte[][] p) {
        StringBuilder sb = new StringBuilder("==== ");
        sb.append(msg).append(" ====\n");
        for (byte[] bytes : d) {
            if(bytes == null) {
                sb.append("NULL");
            } else {
                for (byte b : bytes) {
                    sb.append((int) b).append(" ");
                }
            }
            sb.append("\n");
        }
        for (byte[] bytes : p) {
            if(bytes == null) {
                sb.append("NULL");
            } else {
                for (byte b : bytes) {
                    sb.append((int) b).append(" ");
                }
            }
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }
    
    private static void printIntArray(String msg, int[] is) {
        StringBuilder sb = new StringBuilder(msg);
        sb.append(" : ");
        for(int i : is) {
            sb.append(i).append(" ");
        }
        sb.append("\n");
        System.out.println(sb.toString());
    }
    
    private static void fix(ErasureCode code, byte[][] datas, byte[][] codes, int buf, int[] erased) {
        try {
            printIntArray("Try to Fix", erased);
            int[] locations = code.locationsToReadForDecode(erased);
            printIntArray("Locations", locations);
            byte[][] newDatas = new byte[datas.length][];
            byte[][] newCodes = new byte[codes.length][];
            for(int loc : locations) {
                if(loc < datas.length) {
                    newDatas[loc] = datas[loc];
                } else {
                    newCodes[loc - datas.length] = codes[loc - datas.length];
                }
            }
            for(int er : erased) {
                if(er < datas.length) {
                    newDatas[er] = new byte[buf];
                } else {
                    newCodes[er - datas.length] = new byte[buf];
                }
            }
            printData("Before fixed", newDatas, newCodes);
            code.decode(newDatas, newCodes, buf, erased);
            printData("After fixed", newDatas, newCodes);
        } catch(TooManyErasedLocations ex) {
            ex.printStackTrace();
        }
    }
    

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        ErasureCode code = new JeraRSCode();
        code.initialize(3, 2);
        int bufSize = 16;
        byte[][] datas = new byte[3][];
        for (int i = 0; i < 3; i++) {
            datas[i] = new byte[bufSize];
            Arrays.fill(datas[i], (byte) (100 + i));
        }
        byte[][] codes = new byte[2][];
        for (int i = 0; i < 2; i++) {
            codes[i] = new byte[bufSize];
            Arrays.fill(codes[i], (byte) 0);
        }

        printData("Before Encode", datas, codes);
        code.encode(datas, codes, bufSize);
        printData("After Encode", datas, codes);
        
        int[] errOne = new int[1];
        for(int i = 0; i < 5; i++) {
            errOne[0] = i;
            fix(code, datas, codes, bufSize, errOne);
        }
        
        int[] erased = new int[2];
        erased[0] = 0; erased[1] = 1;
        fix(code, datas, codes, bufSize, erased);
        
        erased[0] = 0; erased[1] = 2;
        fix(code, datas, codes, bufSize, erased);
        
        erased[0] = 1; erased[1] = 4;
        fix(code, datas, codes, bufSize, erased);
        
        erased[0] = 3; erased[1] = 4;
        fix(code, datas, codes, bufSize, erased);
    }
}
