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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.ExportUtils;

/**
 *
 * @author padicao
 */
public class TestBlockIDs {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        DistributedFileSystem dfs = (DistributedFileSystem)FileSystem.get(conf);
        Path p = dfs.makeQualified(new Path("text/part-m-00000"));
        p = Path.getPathWithoutSchemeAndAuthority(p);
        long[] blockIDs = ExportUtils.getBlockIDs(dfs, p.toString(), 8 * dfs.getBlockSize(p));
    }
}
