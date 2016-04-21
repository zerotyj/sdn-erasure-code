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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

/**
 *
 * @author padicao
 */
public class ExportUtils {
    public static long[] getBlockIDs(DistributedFileSystem hdfs, 
            String filepath, long length) throws IOException {
        //System.out.println(String.format("%s : %d", filepath, length));
        ClientProtocol namenode = hdfs.getClient().getNamenode();
        LocatedBlocks blocks = namenode.getBlockLocations(filepath, 0, length);
        List<LocatedBlock> locatedBlocks = blocks.getLocatedBlocks();
        long[] blockIDs = new long[locatedBlocks.size()];
        int index = 0;
        for(LocatedBlock b : locatedBlocks) {
            //System.out.println(String.format("Offset %d ID %d", 
            //        b.getStartOffset(), b.getBlock().getBlockId()));
            blockIDs[index] = b.getBlock().getBlockId();
            index++;
        }
        return blockIDs;
    }
}
