package cn.edu.hust.tang.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.edu.hust.tang.sdnnetwork.ECClientInputStream;
import cn.edu.hust.xie.localityencode.raidfs.RaidfsInputStream;

public class TestNormalRead {

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = fs.makeQualified(new Path("text1G/part-m-00000"));
//        Path path2 = fs.makeQualified(new Path("text1G/part-m-00000"));
        System.out.println("Path " + path.toString());
//        ECClientInputStream input = new ECClientInputStream(fs, path, 128 * 1024);
//        RaidfsInputStream input = new RaidfsInputStream(fs, path, 128 * 1024);
        FSDataInputStream input = fs.open(path);
//       FSDataInputStream input2 = fs.open(path2);
//        input.setSimulate(true);
        byte[] buffer = new byte[4*1024];
        
        long t1 = System.currentTimeMillis();
        while (true) {
        	int haveread = input.read(buffer);
        	if (haveread == -1) {
  //      		while (true) {
//			int haveread2 = input2.read(buffer);
//			if (haveread2 == -1)
//				break;
//			}
			break;
        	}
        }
        long t2 = System.currentTimeMillis();
        System.out.printf("Generate data:\t%d\n", (t2 - t1));
        
        input.close();
	}

}
