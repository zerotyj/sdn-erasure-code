package cn.edu.hust.tang.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import cn.edu.hust.xie.localityencode.raidfs.RaidfsInputStream;

import cn.edu.hust.tang.sdnnetwork.ECClientInputStream;

public class TestECClientInputStream {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = fs.makeQualified(new Path("text1G/part-m-00000"));
        System.out.println("Path " + path.toString());
        ECClientInputStream input = new ECClientInputStream(fs, path, 128 * 1024);
        input.setSimulate(true);
        byte[] buffer = new byte[4096];

	     FSDataInputStream input2 = fs.open(path);
//	RaidfsInputStream input = new RaidfsInputStream(fs, path, 128 * 1024);
//	input.setSimulate(true);
        byte[] buffer2 = new byte[4096];

	int j = 0;
	int k = 0;
        while (true) {
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
			input.close();
			input2.close();
                    return ;
                }
            }
		j++;
            if (j == 32768) {
            	System.out.println("have read a block!");
            	j = 0;
            	k++;
	    }
        }
	System.out.println("have read " + k + " block");
        System.out.println("Read DONE");

        input.close();
        input2.close();
	}

}
