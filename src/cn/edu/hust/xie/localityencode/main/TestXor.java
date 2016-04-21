/*
 * Copyright 2015 padicao.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.hust.xie.localityencode.main;

import cn.edu.hust.xie.localityencode.main.ec.ErasureCode;
import cn.edu.hust.xie.localityencode.main.ec.XorCode;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 *
 * @author padicao
 */
public class TestXor {

    private ErasureCode code;
    private byte[][] datas;
    private int bufSize = 10;
    private Random rand;

    private TestXor() {
        rand = new Random();
        code = new XorCode();
        code.initialize(3, 2);

        datas = new byte[code.stripeCount() + code.parityCount()][];
        for (int i = 0; i < datas.length; i++) {
            datas[i] = new byte[bufSize];
        }
        for(int i = 0; i < code.stripeCount(); i++) {
            Arrays.fill(datas[i], (byte)rand.nextInt(256));
        }
    }

    private void printData() {
        StringBuilder sb = new StringBuilder("-----------------------\n");
        for (byte[] data : datas) {
            for (byte b : data) {
                sb.append(b & 0xff).append(" ");
            }
            sb.append("\n");
        }
        sb.append("-----------------------\n");
        System.out.println(sb.toString());
    }

    private void doEncode() throws IOException {
        byte[][] messages = new byte[code.stripeCount()][];
        System.arraycopy(datas, 0, messages, 0, code.stripeCount());
        byte[][] parities = new byte[code.parityCount()][];
        System.arraycopy(datas, code.stripeCount(), parities, 0, code.parityCount());
        code.encode(messages, parities, bufSize);
        printData();
    }

    private void doDecode() throws IOException {
        byte[][] messages = new byte[code.stripeCount()][];
        byte[][] parities = new byte[code.parityCount()][];

        byte[] outbuf = new byte[bufSize];
        int[] erased = new int[1];
        for (int i = 2; i >= 2; i--) {
            erased[0] = i;
            int[] locations = code.locationsToReadForDecode(erased);
            for(int loc : locations) {
                System.out.print(loc + " ");
            }
            System.out.println("");

            for(int loc : locations) {
                if(loc < code.stripeCount()) {
                    messages[loc] = datas[loc];
                } else {
                    parities[loc - code.stripeCount()] = datas[loc];
                }
            }
            if(erased[0] < code.stripeCount()) {
                messages[erased[0]] = outbuf;
            } else {
                parities[erased[0] - code.stripeCount()] = outbuf;
            }
           
            code.decode(messages, parities, bufSize, erased);
            printData();
            for(byte b : outbuf) {
                System.out.print((b & 0xff) + " ");
            }
            System.out.println("");
        }
    }

    public static void main(String[] args) {
        TestXor test = new TestXor();
        try {
            test.doEncode();
            test.doDecode();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
