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

import cn.edu.hust.xie.localityencode.raidfs.RaidfsInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author padicao
 */
public class TestRaidfsInputStream {

    private static void check(long pos, RaidfsInputStream input, byte[] buffer,
            FSDataInputStream input2, byte[] buffer2) throws IOException {
        input.seek(pos);
        input2.seek(pos);
        for (int i = 0; i < 10; i++) {
            int haveRead = input.read(buffer);
            if (haveRead != 4096) {
                System.err.println("Read not full " + i + ": " + haveRead);
                break;
            } else {
                System.out.println("read " + i + " success");
            }
            for (int j = 0; j < 10; j++) {
                System.out.print((buffer[j] & 0xff) + " ");
            }
            System.out.println("");
            input2.readFully(buffer2);
            for (int j = 0; j < 10; j++) {
                System.out.print((buffer2[j] & 0xff) + " ");
            }
            System.out.println("");
            for (int j = 0; j < 4096; j++) {
                if (buffer[j] != buffer2[j]) {
                    System.err.println("Get wrong data at " + j);
                    return;
                }
            }
            System.out.println("Check finish");
        }
        System.out.println("Read finish");
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = fs.makeQualified(new Path("text1G/part-m-00000"));
        System.out.println("Path " + path.toString());
        RaidfsInputStream input = new RaidfsInputStream(fs, path, 128 * 1024);
        input.setSimulate(true);
        byte[] buffer = new byte[4096];

        FSDataInputStream input2 = fs.open(path);
        byte[] buffer2 = new byte[4096];

        while(true) {
            int haveread = input.read(buffer);
            int haveread2 = input2.read(buffer2);
            if(haveread != haveread2) {
                System.err.println(String.format("read %d %d", haveread, haveread2));
                break;
            }
            if(haveread == -1) {
                break;
            }
            for(int i = 0; i < haveread; i++) {
                if(buffer[i] != buffer2[i]) {
                    System.err.println(String.format("read mismatch at %d %d %d %d %d", i, 
                            input.getPos(), input2.getPos(), buffer[i] & 0xff, buffer2[i] & 0xff));
                    return;
                }
            }
        }
        System.out.println("Read DONE");

        input.close();
        input2.close();
    }
}
