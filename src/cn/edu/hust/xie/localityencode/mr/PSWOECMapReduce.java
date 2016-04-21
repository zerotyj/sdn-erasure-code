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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public class PSWOECMapReduce extends ECMapReduce {

    // 将所有块按节点划分， host => NodeInfo，NodeInfo 包含 Block List
    private Map<String, RackInfo> blocks;
    // 每个块有多个副本， Block => hosts
    private Map<BlockInfo, String[]> blockToRacks;
    // filename => preferRack[]
    private Map<String, String[]> blockPreferRacks;
    private List<Set<String>> stripeBlackList;

    private static final Log LOG = LogFactory.getLog(PSWOECMapReduce.class);

    public PSWOECMapReduce() {
        blocks = new HashMap<String, RackInfo>();
        blockToRacks = new HashMap<BlockInfo, String[]>();
        
        stripeBlackList = new ArrayList<Set<String>>();
        blockPreferRacks = new HashMap<String, String[]>();
    }

    @Override
    protected void initialize(RaidNode node, String path) throws IOException {
        super.initialize(node, path);
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
    }

    private void outputBlockLocation(BlockLocation b) {
        StringBuilder sb = new StringBuilder();
        sb.append(b.getOffset()).append(": HOSTS( ");
        try {
            for (String h : b.getHosts()) {
                sb.append(h).append(" ");
            }
            sb.append(") RACKS( ");
            for (String r : b.getTopologyPaths()) {
                sb.append(r).append(" ");
            }
        } catch (IOException ex) {
            LOG.error(ex);
        }
        sb.append(")");
        LOG.info("Block " + sb.toString());
    }
    
    private void outputBlockInfo(BlockInfo b, String[] racks) {
        StringBuilder sb = new StringBuilder();
        sb.append(b).append(" ( ");
        for(String r : racks) {
            sb.append(r).append(" ");
        }
        sb.append(")");
        LOG.info(sb.toString());
    }
    
    // 遍历目录/文件，获取块位置信息
    private void getBlockLocations() throws IOException {
        for (FileStatus f : encodingFiles) {
            Path p = f.getPath();
            BlockLocation[] locations = fs.getFileBlockLocations(p, 0, f.getLen());
            String filename = isDirectory ? p.getName() : "";
            blockPreferRacks.put(filename, new String[locations.length]);
            for (BlockLocation l : locations) {
                BlockInfo blockInfo = new BlockInfo(filename,
                        (int) (l.getOffset() / blockSize));
                String[] racks = RaidUtils.exactRackFromTPath(l.getTopologyPaths());
                blockToRacks.put(blockInfo, racks);
                for (String r : racks) {
                    RackInfo node = blocks.get(r);
                    if (node == null) {
                        node = new RackInfo(r);
                        blocks.put(r, node);
                    }
                    node.blocks.add(blockInfo);
                }
                outputBlockInfo(blockInfo, racks);
                //System.out.println(l.toString());
            }
        }
    }

    private void outputBlockLocations() {
        for (Map.Entry<String, RackInfo> entry : blocks.entrySet()) {
            System.out.println(String.format("Rack %s has %d blocks",
                    entry.getKey(), entry.getValue().blocks.size()));
        }
    }
    
    private void setPreferRackForBlock(BlockInfo b, String rack) {
        String key = b.getPath();
        String[] racks = blockPreferRacks.get(key);
        if(racks == null) {
            LOG.error("Can't set Prefer Rack for file " + key);
            return;
        }
        racks[b.getBlockIndex()] = rack;
    }
    
    private void setupPreferRackMap() throws IOException {
        Map<String, Integer> finalRackMap = new HashMap<String, Integer>();
        
        for(Map.Entry<String, String[]> entry : blockPreferRacks.entrySet()) {
            String name = entry.getKey();
            String[] racks = entry.getValue();
            
            Path p = Path.getPathWithoutSchemeAndAuthority(new Path(this.dataPath, name));
            long[] blockids = ExportUtils.getBlockIDs((DistributedFileSystem)fs, p.toString(), blockSize * racks.length);
            for(int i = 0; i < blockids.length; i++) {
                //LOG.info(String.format("SetupPreferRackMap %d %s", blockids[i], racks[i]));
                preferRackMap.put(blockids[i], racks[i]);
                
                // 统计各机架的数据块数目
                Integer count = finalRackMap.get(racks[i]);
                if(count == null) {
                    count = 1;
                }else {
                    count = count + 1;
                }
                finalRackMap.put(racks[i], count);
            }
        }
        
        for(Map.Entry<String, Integer> entry : finalRackMap.entrySet()) {
            LOG.info(String.format("Rack %s : %d", entry.getKey(), entry.getValue()));
        }
        LOG.info(String.format("Co-located blocks : %d", 0));
    }

    private BlockInfo selectBlock(RackInfo selectRack, Set<String> blacklistRacks) {
        for (BlockInfo b : selectRack.blocks) {
            for (String r : blockToRacks.get(b)) {
                if (!blacklistRacks.contains(r)) {
                    setPreferRackForBlock(b, r);
                    blacklistRacks.add(r);
                    return b;
                }
            }
        }
        return null;
    }

    private BlockInfo selectOtherBlock(RackInfo selectRack, Set<String> blacklistRacks) {
        for (RackInfo rack : blocks.values()) {
            if (!rack.blocks.isEmpty()
                    && !blacklistRacks.contains(rack.rack)) {
                BlockInfo block = rack.blocks.get(0);
                setPreferRackForBlock(block, rack.rack);
                blacklistRacks.add(rack.rack);
                return block;
            }
        }
        return null;
    }

    private void setStripeBlackList(int index, Set<String> blacklist) {
        stripeBlackList.add(blacklist);
    }
    
    private int selectRackFull(RackInfo[] racks, int lastIndex) {
        for(int i = 0; i < racks.length; i++) {
            int index = (lastIndex + 1 + i) % racks.length;
            if(racks[index].blocks.size() > erasureCode.stripeCount()) {
                return index;
            }
        }
        return -1;
    }
    
    private int selectRackPart(RackInfo[] racks, int lastIndex) {
        for(int i = 0; i < racks.length; i++) {
            int index = (lastIndex + 1 + i) % racks.length;
            if(racks[index].blocks.size() > 0) {
                return index;
            }
        }
        return -1;
    }
    //按机机架进行
    @Override
    public void selectTask() throws IOException {
        getBlockLocations();
        outputBlockLocations();

        int stripeIndex = 0;
        int lastRackIndex = 0;
        RackInfo[] rackArray = blocks.values().toArray(new RackInfo[0]);
        while (true) {
            int selectRackIndex = selectRackFull(rackArray, lastRackIndex);
            if(selectRackIndex < 0) {
                selectRackIndex = selectRackPart(rackArray, lastRackIndex);
            }
            
            if(selectRackIndex < 0) {
                break;
            }
            RackInfo selectRack = rackArray[selectRackIndex];
            lastRackIndex = selectRackIndex;
            
            String host = node.getRandomHostInRack(selectRack.rack);
            //LOG.info(String.format("Select %s @ %s", selectRack.rack, host));
            TaskInfo t = new TaskInfo(host, stripeIndex);

            Set<String> blacklistRacks = new HashSet<String>();
            for (int i = 0; i < erasureCode.stripeCount(); i++) {
                BlockInfo selectedBlock = null;
                if (!selectRack.blocks.isEmpty()) {
                    selectedBlock = selectBlock(selectRack, blacklistRacks);
                }
                if (selectedBlock == null) {
                    selectedBlock = selectOtherBlock(selectRack, blacklistRacks);
                }
                if (selectedBlock == null) {
                    break;
                }
                t.addBlock(selectedBlock);
                String[] racks = blockToRacks.get(selectedBlock);
                for (String r : racks) {
                    RackInfo rack = blocks.get(r);
                    rack.blocks.remove(selectedBlock);
                }
            }
            setStripeBlackList(t.getStripeIndex(), blacklistRacks);
            RaidUtils.addEmptyBlock(t, erasureCode.stripeCount());
            this.addTask(t);
            stripeIndex++;
        }
        setParityReplication(1);
        setCodeDistirubtion(true);
        node.addStripeBlackList(dataPath.toString(), stripeBlackList);
    }
    
    // 构建条带，创建任务
    /*
    @Override
    public void selectTask() throws IOException {
        getBlockLocations();
        outputBlockLocations();

        int stripeIndex = 0;
        while (true) {
            RackInfo selectRack = Collections.max(blocks.values());
            
            if (selectRack.blocks.isEmpty()) {
                break;
            }
            String host = node.getRandomHostInRack(selectRack.rack);
            LOG.info(String.format("Select %s @ %s", selectRack.rack, host));
            TaskInfo t = new TaskInfo(host, stripeIndex);

            Set<String> blacklistRacks = new HashSet<String>();
            for (int i = 0; i < erasureCode.stripeCount(); i++) {
                BlockInfo selectedBlock = null;
                if (!selectRack.blocks.isEmpty()) {
                    selectedBlock = selectBlock(selectRack, blacklistRacks);
                }
                if (selectedBlock == null) {
                    selectedBlock = selectOtherBlock(selectRack, blacklistRacks);
                }
                if (selectedBlock == null) {
                    break;
                }
                t.addBlock(selectedBlock);
                String[] racks = blockToRacks.get(selectedBlock);
                for (String r : racks) {
                    RackInfo rack = blocks.get(r);
                    rack.blocks.remove(selectedBlock);
                }
            }
            setStripeBlackList(t.getStripeIndex(), blacklistRacks);
            RaidUtils.addEmptyBlock(t, erasureCode.stripeCount());
            this.addTask(t);
            stripeIndex++;
        }
        setParityReplication(1);
        setCodeDistirubtion(true);
        node.addStripeBlackList(dataPath.toString(), stripeBlackList);
    }
    */

    @Override
    protected void adjustReplication() throws IOException {
        setupPreferRackMap();
        node.removeStripeBlackList(dataPath.toString());
        super.adjustReplication();
    }
}
