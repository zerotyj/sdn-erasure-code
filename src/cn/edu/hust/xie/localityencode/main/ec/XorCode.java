/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hust.xie.localityencode.main.ec;

import java.util.Arrays;

/**
 *
 * @author yvanhom
 */
public class XorCode implements ErasureCode {

    private int stripe;

    @Override
    public void initialize(int s, int p) {
        stripe = s;
    }

    @Override
    public int stripeCount() {
        return stripe;
    }

    @Override
    public int parityCount() {
        return 1;
    }
    
    private void doXor(byte[][] sources, byte[] target, int length) {
        Arrays.fill(target, 0, length,(byte) 0);
        for (byte[] s : sources) {
            for (int i = 0; i < length; i++) {
                target[i] ^= s[i];
            }
        }
    }

    @Override
    public void encode(byte[][] datas, byte[][] parities, int length) {
        doXor(datas, parities[0], length);
    }

    @Override
    public int[] locationsToReadForDecode(int[] erased) throws TooManyErasedLocations {
        if (erased.length != 1) {
            throw new TooManyErasedLocations("Number of erased locations should be 1 : " + erased.length);
        }
        int[] result = new int[stripe];
        int index = 0;
        for (int i = 0; i < stripe + 1; i++) {
            if (i == erased[0]) {
                continue;
            }
            result[index] = i;
        }

        return result;
    }

    @Override
    public void decode(byte[][] datas, byte[][] parities, int length, int[] erased) {
        try {
            int[] locations = locationsToReadForDecode(erased);
            byte[][] decodeDatas = new byte[stripe][];
            for (int i = 0; i < stripe; i++) {
                if (locations[i] < stripe) {
                    decodeDatas[i] = datas[locations[i]];
                } else {
                    decodeDatas[i] = parities[0];
                }
            }
            byte[] target;
            if (erased[0] < stripe) {
                target = datas[erased[0]];
            } else {
                target = parities[0];
            }

            doXor(decodeDatas, target, length);
        } catch (TooManyErasedLocations ex) {
        }
    }

}
