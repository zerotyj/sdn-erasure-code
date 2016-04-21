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

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

/**
 *
 * @author padicao
 */
public class TestWriteOneNode {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
        Path path = fs.makeQualified(new Path("1.txt"));
        InetSocketAddress[] favoredNodes = new InetSocketAddress[1];
        String dnAddr = conf.get("dfs.datanode.address", "0.0.0.0:50010");
        int port = Integer.valueOf(dnAddr.substring(dnAddr.lastIndexOf(":") + 1));
        
        System.out.println("Create file set favored at slave7:" + port);
        
        favoredNodes[0] = new InetSocketAddress("slave7", port);
        HdfsDataOutputStream output = fs.create(path, FsPermission.getFileDefault().applyUMask(
                FsPermission.getUMask(conf)), true, conf.getInt("io.file.buffer.size", 4096),
                (short)1, fs.getDefaultBlockSize(), null, favoredNodes);
        byte[] buffer = new byte[64 * 1024];
        output.write(buffer);
        output.flush();
        output.close();
        
        System.out.println("finish write, and get block locations");
        
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(path, 0, fs.getDefaultBlockSize());
        for(BlockLocation b : fileBlockLocations) {
            System.out.println(b.toString());
        }
        fs.close();
    }
}
