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

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 *
 * @author padicao
 */
public class TestBlockLocations {

    private static void getPathBlockLocations(FileContext fc, Path p, FileStatus status) throws IOException {
        if(status == null) {
            status = fc.getFileStatus(p);
        }
        if (status.isDirectory()) {
            RemoteIterator<FileStatus> iter = fc.listStatus(p);
            while(iter.hasNext()) {
                FileStatus sub = iter.next();
                getPathBlockLocations(fc, sub.getPath(), sub);
            }
        } else {
            BlockLocation[] fileBlockLocations = fc.getFileBlockLocations(p, 0, status.getLen());
            System.out.println("PATH:" + p.getName());
            for (BlockLocation b : fileBlockLocations) {
                System.out.println(b.toString());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage TestBlockLocations path");
            return;
        }
        Configuration conf = new Configuration();
        FileContext fc = FileContext.getFileContext(conf);
        Path filePath = fc.makeQualified(new Path(args[0]));
        getPathBlockLocations(fc, filePath, null);
    }
}
