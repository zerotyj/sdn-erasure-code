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
package cn.edu.hust.xie.localityencode.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.ExportUtils;

/**
 *
 * @author padicao
 */
public class RandomECMapReduce extends ECMapReduce {

    private static final Log LOG = LogFactory.getLog(RandomECMapReduce.class);
    
    private static String RANDOM_PARITY_REPLICAS = "mapreduce.random.parity.replicas";

    // 每个条带内每个块对应的 BlockID
    private List<List<Long>> stripeBlockIDs = new ArrayList<List<Long>>();
    // 每个条带内，每个机架拥有的块数目
    private List<Map<String, Integer>> blockCountInRack = new ArrayList<Map<String, Integer>>();
    // 每个条带内每个块所在的机架
    private List<List<List<String>>> blockMaps = new ArrayList<List<List<String>>>();

    // 设置该任务的本地执行节点，对于 Random 算法，便是不需要设置
    protected void setupPreferHost(TaskInfo t, 
            Map<String, Integer> rackMap, Map<String, Integer> nodeMap) {

    }

    private void updateStripeInfo(TaskInfo t) throws IOException {
        int size = t.getBlocks().size();

        // 初始化
        Map<String, Integer> rackMap = new HashMap<String, Integer>();
        Map<String, Integer> nodeMap = new HashMap<String, Integer>();
        List<List<String>> blockMap = new ArrayList<List<String>>();
        
        // 将每个块的信息加入统计
        for (BlockInfo b : t.getBlocks()) {
            Path p = new Path(this.dataPath, b.getPath());
            BlockLocation[] locs = fs.getFileBlockLocations(p, b.getBlockIndex() * blockSize, blockSize);
            updateBlockLocation(locs[0], rackMap, nodeMap, blockMap);
        }
        blockCountInRack.add(rackMap);
        blockMaps.add(blockMap);
        
        // 设置该任务的本地执行节点
        setupPreferHost(t, rackMap, nodeMap);
    }

    // 一个块多个副本在多个机架上，选择块最多的机架上的副本，并丢弃
    private void reduceReplica(List<String> racks, Map<String, Integer> blockcounts) {
        String selected = null;
        int selectCount = 0;
        for (String r : racks) {
            int count = blockcounts.get(r);
            if (selectCount < count) {
                selectCount = count;
                selected = r;
            }
        }
        racks.remove(selected);
        blockcounts.put(selected, selectCount - 1);
    }

    // 用 BlockLocation 更新信息
    protected void updateBlockLocation(BlockLocation l,
            Map<String, Integer> rackMap, Map<String, Integer> nodeMap, 
            List<List<String>> blockMap) throws IOException {
        String[] rs = RaidUtils.exactRackFromTPath(l.getTopologyPaths());
        List<String> blockracks = new ArrayList<String>();
        for (String r : rs) {
            blockracks.add(r);
            Integer count = rackMap.get(r);
            if (count == null) {
                count = 1;
            } else {
                count++;
            }
            rackMap.put(r, count);
        }
        blockMap.add(blockracks);
        
        if(nodeMap != null) {
            for(String h : l.getHosts()) {
                Integer count = nodeMap.get(h);
                if(count == null) {
                    count = 1;
                } else {
                    count++;
                }
                nodeMap.put(h, count);
            }
        }
    }

    // 设置每个块的倾向机架
    private void setupPreferRackMap() throws IOException {
        int parityCount = erasureCode.parityCount();
        int size = stripeBlockIDs.size();
        long parityLength = size * parityCount * blockSize;
        long[] blockids = ExportUtils.getBlockIDs((DistributedFileSystem) fs,
                Path.getPathWithoutSchemeAndAuthority(this.finalParityPath).toString(),
                parityLength);

        BlockLocation[] locs = fs.getFileBlockLocations(this.finalParityPath, 0, parityLength);
        
        int colocatedBlockCount = 0;
        Map<String, Integer> finalRackMap = new HashMap<String, Integer>();

        for (int i = 0; i < size; i++) {
            List<Long> bids = stripeBlockIDs.get(i);
            Map<String, Integer> rackMap = blockCountInRack.get(i);
            List<List<String>> blockMap = blockMaps.get(i);
            
            // 将编码块的信息加入统计
            for (int j = 0; j < parityCount; j++) {
                int index = i * parityCount + j;
                bids.add(blockids[index]);
                updateBlockLocation(locs[index], rackMap, null, blockMap);
            }
            // 逐渐减少副本数目
            for (List<String> rs : blockMap) {
                for (int j = rs.size(); j > 1; j--) {
                    reduceReplica(rs, rackMap);
                }
            }
            
            // 统计各机架的数据块数目
            for(int j = 0; j < erasureCode.stripeCount(); j++) {
                String rack = blockMap.get(j).get(0);
                Integer count = finalRackMap.get(rack);
                if(count == null) {
                    count = 1;
                }else {
                    count = count + 1;
                }
                finalRackMap.put(rack, count);
            }
            
            // 计算跨机架流量
            for(Integer cr : rackMap.values()) {
                if(cr > 1) {
                    colocatedBlockCount += cr - 1;
                }
            }

            // 设置
            for (int j = 0; j < bids.size(); j++) {
                long id = bids.get(j);
                String rack = blockMap.get(j).get(0);
                this.preferRackMap.put(id, rack);
                //LOG.info(String.format("PreferRackMap %d %s", id, rack));
            }
        }
        for(Map.Entry<String, Integer> entry : finalRackMap.entrySet()) {
            LOG.info(String.format("Rack %s : %d", entry.getKey(), entry.getValue()));
        }
        LOG.info(String.format("Co-located blocks : %d", colocatedBlockCount));
    }
    
    private void updateParityReplications() {
        int pr = this.conf.getInt(RANDOM_PARITY_REPLICAS, 3);
        this.setParityReplication(pr);
    }

    // 遍历目录/文件，选择条带
    @Override
    public void selectTask() throws IOException {
        TaskInfo t = null;
        int stripeIndex = 0;
        int numOfBlocks = 0;
        List<Long> stripeBIDs = null;
        for (FileStatus f : encodingFiles) {
            Path p = f.getPath();
            long len = f.getLen();
            int blockCount = (int) ((len + blockSize - 1) / blockSize);
            numOfBlocks += blockCount;

            long[] blockids = ExportUtils.getBlockIDs((DistributedFileSystem) fs,
                    Path.getPathWithoutSchemeAndAuthority(p).toString(), blockCount * blockSize);

            for (int i = 0; i < blockCount; i++) {
                if (t == null) {
                    t = new TaskInfo("", stripeIndex);
                    stripeBIDs = new ArrayList<Long>();
                    stripeBlockIDs.add(stripeBIDs);
                    stripeIndex++;
                }

                stripeBIDs.add(blockids[i]);
                t.addBlock(new BlockInfo(isDirectory ? p.getName() : "", i));
                if (t.getBlocks().size() == erasureCode.stripeCount()) {
                    updateStripeInfo(t);
                    this.addTask(t);
                    t = null;
                }
            }
        }
        if (t != null) {
            updateStripeInfo(t);
            RaidUtils.addEmptyBlock(t, erasureCode.stripeCount());
            this.addTask(t);
            stripeIndex++;
        }

        updateParityReplications();
        LOG.info("Numer of Blocks : " + numOfBlocks);
    }

    @Override
    protected void adjustReplication() throws IOException {
        setupPreferRackMap();
        super.adjustReplication();
    }
}
