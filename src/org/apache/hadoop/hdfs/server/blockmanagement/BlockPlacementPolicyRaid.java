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
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

/**
 *
 * @author padicao
 */
public class BlockPlacementPolicyRaid extends BlockPlacementPolicyDefault {

    private BlockPlacementServer server;
    private final static Log LOG = LogFactory.getLog(BlockPlacementPolicyRaid.class);
    private static Random random = new Random();
    private boolean dist_3rr = false;

    @Override
    public void initialize(Configuration conf, FSClusterStats stats,
            NetworkTopology clusterMap,
            Host2NodesMap host2datanodeMap) {
        super.initialize(conf, stats, clusterMap, host2datanodeMap);
        LOG.info("Initialize BlockPlacementPolicyRaid");
        try {
            server = BlockPlacementServer.getInstance(conf);
        } catch (Exception ex) {
            server = null;
            LOG.info(ex);
        }
        dist_3rr = conf.getBoolean(RaidConfig.DIST_3RR, false);
    }

    private EnumMap<StorageType, Integer> getRequiredStorageTypes(
            List<StorageType> types) {
        EnumMap<StorageType, Integer> map = new EnumMap<StorageType, Integer>(StorageType.class);
        for (StorageType type : types) {
            if (!map.containsKey(type)) {
                map.put(type, 1);
            } else {
                int num = map.get(type);
                map.put(type, num + 1);
            }
        }
        return map;
    }

    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath,
            int numOfReplicas,
            Node writer,
            List<DatanodeStorageInfo> chosen,
            boolean returnChosenNodes,
            Set<Node> excludedNodes,
            long blocksize,
            BlockStoragePolicy storagePolicy) {
        if (!dist_3rr || numOfReplicas > this.clusterMap.getNumOfRacks()) {
            return super.chooseTarget(srcPath, numOfReplicas,
                    writer, chosen,
                    returnChosenNodes, excludedNodes,
                    blocksize, storagePolicy);
        }
        
        if (excludedNodes == null) {
            excludedNodes = new HashSet<Node>();
        }

        final List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>(chosen);
        for (DatanodeStorageInfo storage : chosen) {
            // add localMachine and related nodes to excludedNodes
            addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
        }

        final List<StorageType> requiredStorageTypes = storagePolicy.chooseStorageTypes((short) numOfReplicas);
        final EnumMap<StorageType, Integer> storageTypes
                = getRequiredStorageTypes(requiredStorageTypes);

        try {
            this.chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize, 1, results, false, storageTypes);
        } catch (NotEnoughReplicasException ex) {
            LOG.error(ex);
            return super.chooseTarget(srcPath, numOfReplicas,
                    writer, chosen,
                    returnChosenNodes, excludedNodes,
                    blocksize, storagePolicy);
        }
        return results.toArray(new DatanodeStorageInfo[results.size()]);
    }

    @Override
    public DatanodeStorageInfo chooseReplicaToDelete(BlockCollection bc,
            Block block, short replicationFactor,
            Collection<DatanodeStorageInfo> first,
            Collection<DatanodeStorageInfo> second,
            final List<StorageType> excessTypes) {
        long blockID = block.getBlockId();
        String preferRack = (server != null ? server.getPreferRack(blockID) : null);
        if (preferRack == null) {
            LOG.info("Use default BlockPlacementPolicy");
            return super.chooseReplicaToDelete(bc, block, replicationFactor, first, second, excessTypes);
        }

        LOG.info("Use BlockPlacementPolicyRaid prefer " + preferRack);
        DatanodeStorageInfo storage = null;
        for (DatanodeStorageInfo d : first) {
            String dr = d.getDatanodeDescriptor().getNetworkLocation();
            if (!dr.startsWith(preferRack)) {
                storage = d;
                LOG.info(String.format("choose %s from FIRST", dr));
                break;
            }
        }

        if (storage == null) {
            for (DatanodeStorageInfo d : second) {
                String dr = d.getDatanodeDescriptor().getNetworkLocation();
                if (!dr.startsWith(preferRack)) {
                    LOG.info(String.format("choose %s from SECOND", dr));
                    storage = d;
                    break;
                }
            }
        }

        if (storage == null) {
            int rnd = random.nextInt(first.size() + second.size());
            if (rnd < first.size()) {
                for (DatanodeStorageInfo d : first) {
                    if (rnd == 0) {
                        storage = d;
                        break;
                    }
                    rnd--;
                }
            } else {
                rnd -= first.size();
                for (DatanodeStorageInfo d : second) {
                    if (rnd == 0) {
                        storage = d;
                        break;
                    }
                    rnd--;
                }
            }
        }

        if (storage != null) {
            if (excessTypes.contains(storage.getStorageType())) {
                LOG.info("choose node from excessTypes");
                excessTypes.remove(storage.getStorageType());
            } else {
                LOG.info("choose node not from excessTypes");
            }
        }
        return storage;
    }
}
