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
package org.apache.hadoop.hdfs.server.blockmanagement;

import cn.edu.hust.xie.localityencode.mr.RaidConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

/**
 *
 * @author padicao
 */
public class BlockPlacementServer implements BlockPlacementProtocol {

    private RPC.Server server;
    private Map<Long, String> preferRackMap;
    private final static Log LOG = LogFactory.getLog(BlockPlacementServer.class);
    
    private static BlockPlacementServer onlyone;
    
    public void initialize(Configuration conf) throws IOException, NullPointerException {
        preferRackMap = new HashMap<Long, String>();
        
        String hostport = conf.get(RaidConfig.BLOCK_PLACEMENT_ADDRESS);
        LOG.info("Host: port " + hostport);
        String[] ss = hostport.split(":");
        String host = ss[0];
        int port = Integer.valueOf(ss[1]);
        this.server = new RPC.Builder(conf)
                .setProtocol(BlockPlacementProtocol.class)
                .setInstance(this).setBindAddress(host)
                .setPort(port)
                .setNumHandlers(1).build();
        server.start();
    }

    @Override
    public void setBlockPreferRacks(BlockRackMap map) throws IOException {
        Map<Long, String> preferRacks = map.getPreferRacks();
        LOG.info(String.format("Get %d map entries", preferRacks.size()));
        synchronized (preferRackMap) {
            preferRackMap.putAll(preferRacks);
        }
    }
    
    public static BlockPlacementServer getInstance(Configuration conf) throws IOException {
        if (onlyone == null) {
            onlyone = new BlockPlacementServer();
            onlyone.initialize(conf);
        }
        return onlyone;
    }
    
    @Override
    public void cleanCache() {
        synchronized(preferRackMap) {
            preferRackMap.clear();
        }
    }
    
    public String getPreferRack(long blockid) {
        synchronized(preferRackMap) {
            return preferRackMap.get(blockid);
        }
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
            long clientVersion, int clientMethodsHash) throws IOException {
        return new ProtocolSignature(versionID, null);
    }

    public static BlockPlacementProtocol getBlockPlacementClient(Configuration conf) throws IOException {
        String hostport = conf.get(RaidConfig.BLOCK_PLACEMENT_ADDRESS);
        String[] ss = hostport.split(":");

        return (BlockPlacementProtocol) RPC.getProxy(BlockPlacementProtocol.class,
                BlockPlacementProtocol.versionID, new InetSocketAddress(ss[0], Integer.valueOf(ss[1])),
                conf);
    }
    
    public void close() {
        if(server != null) {
            server.stop();
            server = null;
        }
        preferRackMap.clear();
    }
}
