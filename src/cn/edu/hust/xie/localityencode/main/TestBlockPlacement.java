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
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockRackMap;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementProtocol;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementServer;
import org.apache.hadoop.ipc.RPC;

/**
 *
 * @author padicao
 */
public class TestBlockPlacement {
    public static void main(String[] args) throws IOException {
        BlockPlacementServer server = new BlockPlacementServer();
        Configuration conf = new Configuration();
        server.initialize(conf);
        
        BlockPlacementProtocol client = BlockPlacementServer.getBlockPlacementClient(conf);
        Map<Long, String> map = new HashMap<Long, String>();
        map.put(123456L, "slave1");
        map.put(234556L, "slave8");
        BlockRackMap req = new BlockRackMap(map);
        client.setBlockPreferRacks(req);
        
        System.out.println(server.getPreferRack(123456L));
        RPC.stopProxy(client);
        server.close();
    }
}
