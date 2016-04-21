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

import cn.edu.hust.xie.localityencode.main.ec.ErasureCode;
import cn.edu.hust.xie.localityencode.mr.ECMapReduce.MRInputKey;
import cn.edu.hust.xie.localityencode.mr.ECMapReduce.MRInputValue;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementProtocol;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementServer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

/**
 *
 * @author padicao
 */
public class RaidNode implements RaidNodeProtocol {

    public static final Log LOG = LogFactory.getLog(RaidNode.class);
    public static final Random random = new Random();

    private Configuration conf;
    private FileSystem fileSystem;
    private LinkedList<RaidJob> waitingJobs;
    private Map<String, List<String>> rackMap;

    private Map<String, FileStripeInfo> blockMap;
    private Map<String, List<Set<String>>> stripesBlackLists;

    private RaidThread raidThread;

    // 如果文件没有编码，则返回空
    // 如果文件是按文件编码，则返回自己的 Path
    // 如果文件是按目录编码，则返回父目录的 Path
    private Path getDataPathForPath(Path file, boolean isDir) {
        Path parityPath = RaidConfig.getParityPath(file);
        Path metaPath = new Path(parityPath, RaidConfig.getMetaDataName());

        // 直接考虑是否存在 meta data
        LOG.info("Get file Meta data");
        try {
            FileStatus metaStatus = fileSystem.getFileStatus(metaPath);
            if (metaStatus != null) {
                return file;
            }
        } catch (IOException ex) {
            LOG.info("no stripe for this file " + file);
        }

        if (!isDir) {
            // 考虑 file 是不是某个目录下的文件，看该目录是不是有 meta data
            return getDataPathForPath(file.getParent(), true);
        }

        return null;
    }

    private FileStripeInfo getOrAllocFileStripeInfo(Map<String, FileStripeInfo> map, Path filepath) {
        String filename = filepath.toString();
        FileStripeInfo fileStripe = map.get(filename);
        if (fileStripe == null) {
            fileStripe = new FileStripeInfo(filename);
            map.put(filename, fileStripe);
        }
        return fileStripe;
    }

    private Map<String, FileStripeInfo> loadStripeInfoFromMetaFile(Path dataPath) throws IOException {
        Path parityPath = RaidConfig.getParityPath(dataPath);
        Path metaPath = new Path(parityPath, RaidConfig.getMetaDataName());
        Map<String, FileStripeInfo> map = new HashMap<String, FileStripeInfo>();
        ErasureCode ec = RaidConfig.getErasureCode(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(metaPath));
        MRInputKey key = new MRInputKey();
        MRInputValue value = new MRInputValue();
        while (reader.next(key, value)) {
            int stripeIndex = key.getStripeIndex();
            List<BlockInfo> blockList = value.getBlockList();

            // 建立条带
            BlockStripeInfo blockStripeInfo = new BlockStripeInfo(stripeIndex);
            // 放数据块
            for (BlockInfo b : blockList) {
                blockStripeInfo.addBlockInfo(b);
            }

            // 将条带按数据块分到各个文件
            for (int i = 0; i < ec.stripeCount(); i++) {
                BlockInfo b = blockList.get(i);
                if (b.equals(BlockInfo.EMPTY_BLOCK_INFO)) {
                    // 跳过空块
                    continue;
                }
                Path newPath = new Path(dataPath, b.getPath());
                FileStripeInfo fileStripe = getOrAllocFileStripeInfo(map, newPath);
                fileStripe.addBlockStripes(b.getBlockIndex(), blockStripeInfo);
            }

            // 将条带放入校验块
            //Path parityFilePath = RaidConfig.getParityFileFinalPath(parityPath);
            //FileStripeInfo parityFileStripe = getOrAllocFileStripeInfo(map, parityFilePath);
            //for (int i = 0; i < ec.paritySize(); i++) {
            //    BlockInfo b = blockList.get(i + ec.stripeSize());
            //    parityFileStripe.addBlockStripes(b.getBlockIndex(), blockStripeInfo);
            //}
        }
        return map;
    }

    private int checkBlock(Path path, int index, long blocksize, Set<String> black) throws IOException {
        BlockLocation[] locs = fileSystem.getFileBlockLocations(path,
                index * blocksize, blocksize);
        String[] rs = RaidUtils.exactRackFromTPath(locs[0].getTopologyPaths());
        for (String r : rs) {
            if (!black.contains(r)) {
                black.add(r);
                return rs.length;
            }
        }
        return -rs.length;
    }

    private int checkDatasetWithMetaData(Path dataPath) throws IOException {
        int result = 0;
        Path parityPath = RaidConfig.getParityPath(dataPath);
        Path metaPath = new Path(parityPath, RaidConfig.getMetaDataName());

        Path parityFile = new Path(parityPath, RaidConfig.getParityFileFinalName());
        ErasureCode ec = RaidConfig.getErasureCode(conf);

        FileStatus parityFileStatus = fileSystem.getFileStatus(parityFile);
        long blockSize = parityFileStatus.getBlockSize();

        LOG.info("Check" + metaPath.toString());
        boolean setReplicaDone = true;
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(metaPath));
        MRInputKey key = new MRInputKey();
        MRInputValue value = new MRInputValue();
        while (reader.next(key, value)) {
            int stripeIndex = key.getStripeIndex();
            List<BlockInfo> blist = value.getBlockList();
            Set<String> racks = new HashSet<String>();
            // 检查数据块
            for (BlockInfo b : blist) {
                if (!b.equals(BlockInfo.EMPTY_BLOCK_INFO)) {
                    int temp = checkBlock(new Path(dataPath, b.getPath()), b.getBlockIndex(), blockSize, racks);
                    if (temp < 0) {
                        result++;
                    }
                    if (setReplicaDone && Math.abs(temp) != 1) {
                        setReplicaDone = false;
                    }
                }
            }
            // 检查校验块
            for (int i = 0; i < ec.parityCount(); i++) {
                int temp = checkBlock(parityFile, stripeIndex * ec.parityCount() + i, blockSize, racks);
                if (temp < 0) {
                    result++;
                }
                if (setReplicaDone && Math.abs(temp) != 1) {
                    setReplicaDone = false;
                }
            }
            LOG.info(String.format("Stripe %d, results %d, %s", stripeIndex, result,
                    setReplicaDone? "DONE" : "Not YET"));
        }
        reader.close();
        return result;
    }

    @Override
    public void clearCache() throws IOException {
        BlockPlacementProtocol blockClient = BlockPlacementServer.getBlockPlacementClient(conf);
        blockClient.cleanCache();
    }

    private class RaidThread extends Thread {

        @Override
        public void run() {
            while (true) {
                RaidJob job;
                try {
                    synchronized (waitingJobs) {
                        while (waitingJobs.isEmpty()) {
                            waitingJobs.wait();
                        }
                        job = waitingJobs.removeFirst();
                    }
                } catch (InterruptedException ex) {
                    return;
                }
                ECMapReduce ecMR;
                switch (job.getType()) {
                    case 0:
                        ecMR = new RandomECMapReduce();
                        break;
                    case 1:
                        ecMR = new LocalityECMapReduce();
                        break;
                    default:
                        ecMR = new PSWOECMapReduce();
                        break;
                }
                try {
                    long startTime = System.nanoTime();
                    ecMR.doRaid(RaidNode.this, job.getFilePath());
                    long endTime = System.nanoTime();
                    LOG.info(String.format("Cost %.2f seconds", (endTime - startTime) / 1000000000.0f));
                } catch (Exception ex) {
                    LOG.error("Can't raid " + job.getFilePath(), ex);
                }
            }

        }
    }

    public Configuration getConf() {
        return conf;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    private void updateRackMap() throws IOException {
        //synchronized (rackMap) {
            rackMap.clear();
            DatanodeInfo[] nodes = ((DistributedFileSystem) fileSystem).getDataNodeStats(HdfsConstants.DatanodeReportType.LIVE);
            for (DatanodeInfo n : nodes) {
                String location = n.getNetworkLocation();
                List<String> hosts = rackMap.get(location);
                //LOG.info(String.format("Current Rack Map: %s %s", location, n.getHostName()));
                if (hosts == null) {
                    hosts = new ArrayList<String>();
                    rackMap.put(location, hosts);
                }
                hosts.add(n.getHostName());
            }
        //}
    }
    
    public List<String> getHostsInRack(String rack) {
        return rackMap.get(rack);
    }

    public String getRandomHostInRack(String rack) {
        //synchronized (rackMap) {
            List<String> hosts = rackMap.get(rack);
            return hosts.get(random.nextInt(hosts.size()));
       // }
    }

    public void initialize(Configuration conf) throws IOException {
        this.conf = conf;
        this.fileSystem = FileSystem.get(conf);
        this.rackMap = new HashMap<String, List<String>>();
        blockMap = new HashMap<String, FileStripeInfo>();
        stripesBlackLists = new HashMap<String, List<Set<String>>>();
        waitingJobs = new LinkedList<RaidJob>();

        updateRackMap();

        raidThread = new RaidThread();
        raidThread.start();
    }

    public void addFileStripeInfo(FileStripeInfo f) {
        synchronized (blockMap) {
            blockMap.put(f.getFilePath(), f);
        }
    }

    public void addStripeBlackList(String path, List<Set<String>> stripeBlackList) {
        synchronized (stripesBlackLists) {
            this.stripesBlackLists.put(path, stripeBlackList);
        }
    }

    public void removeStripeBlackList(String path) {
        synchronized (stripesBlackLists) {
            this.stripesBlackLists.remove(path);
        }
    }

    @Override
    public FileStripeInfo getFileStripeInfo(String file) throws IOException {
        Path dataPath = fileSystem.makeQualified(new Path(file));

        // 先从cache 中查找
        synchronized (blockMap) {
            FileStripeInfo stripe = blockMap.get(dataPath.toString());
            if (stripe != null) {
                return stripe;
            }
        }

        // 如果找不到，则看看该数据是否有 Raid
        LOG.info("Can't get from  cache, check HDFS");
        Path raidDataPath = getDataPathForPath(dataPath, false);
        if (raidDataPath == null) {
            return null;
        }

        LOG.info("Load from HDFS");
        // 有 Raid， 从文件中加载条带信息，并放进 cache中
        Map<String, FileStripeInfo> nmap = loadStripeInfoFromMetaFile(raidDataPath);

        LOG.info("Get again");
        // 重新获取
        synchronized (blockMap) {
            blockMap.putAll(nmap);
            FileStripeInfo stripe = blockMap.get(dataPath.toString());
            return stripe;
        }
    }

    @Override
    public BlockStripeInfo getBlockStripeInfo(String filePath, int blockIndex) throws IOException {
        if (blockIndex < 0) {
            return null;
        }
        FileStripeInfo stripe = getFileStripeInfo(filePath);
        if (stripe == null) {
            return null;
        }
        return stripe.getBlockStripe(blockIndex);
    }

    private void outputCodeBlockDebug(String path, int index, Set<String> black, String[] results) {
        StringBuilder sb = new StringBuilder();
        sb.append(path).append(" ").append(index).append(" BLACK( ");
        for (String s : black) {
            sb.append(s).append(" ");
        }
        sb.append(") RESULTS( ");
        for (String r : results) {
            sb.append(r).append(" ");
        }
        sb.append(")");
        LOG.info(sb.toString());
    }

    @Override
    public String[] getCodeBlockPreferHost(String filePath, int stripeIndex, int count) {
        LOG.info(String.format("getCodeBlockPreferHost %s, %d, %d", filePath, stripeIndex, count));
        List<Set<String>> sbls;
        synchronized (stripesBlackLists) {
            sbls = stripesBlackLists.get(filePath);
            if (sbls == null) {
                return null;
            }
        }
        Set<String> blacklist = sbls.get(stripeIndex);
        List<String> available = new ArrayList<String>();
        for (String r : rackMap.keySet()) {
            if (!blacklist.contains(r)) {
                available.add(r);
            }
        }
        String[] results = new String[count];
        for (int i = 0; i < count; i++) {
            int rackno = random.nextInt(available.size());
            String rackstr = available.remove(rackno);
            results[i] = getRandomHostInRack(rackstr);
        }
        outputCodeBlockDebug(filePath, stripeIndex, blacklist, results);
        return results;
    }

    public int checkDataset(String path) throws IOException {
        Path dataPath = fileSystem.makeQualified(new Path(path));
        return checkDatasetWithMetaData(dataPath);
    }

    @Override
    public boolean raidFile(String filePath, int type) throws IOException {
        synchronized (waitingJobs) {
            waitingJobs.add(new RaidJob(filePath, type));
            if (waitingJobs.size() == 1) {
                waitingJobs.notify();
            }
        }
        return true;
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

    public static RaidNodeProtocol getRaidNodeClient(Configuration conf) throws IOException {
        String hostport = RaidConfig.getRaidNodeAddress(conf);
        String[] ss = hostport.split(":");

        return (RaidNodeProtocol) RPC.getProxy(RaidNodeProtocol.class,
                RaidNodeProtocol.versionID, new InetSocketAddress(ss[0], Integer.valueOf(ss[1])),
                conf);
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            RaidNode node = new RaidNode();
            node.initialize(conf);

            String hostport = RaidConfig.getRaidNodeAddress(conf);
            String[] ss = hostport.split(":");
            String host = ss[0];
            int port = Integer.valueOf(ss[1]);

            LOG.info(String.format("Start RaidNode at %s:%d", host, port));

            Server server = new RPC.Builder(conf)
                    .setProtocol(RaidNodeProtocol.class)
                    .setInstance(node).setBindAddress(host)
                    .setPort(port)
                    .setNumHandlers(5).build();
            server.start();

            while (!Thread.interrupted()) {
                Thread.sleep(10000);
            }
            server.stop();
            server.join();
            LOG.info("RaidNode shutdown");
        } catch (IOException | HadoopIllegalArgumentException ex) {
            LOG.error(ex);
        } catch (InterruptedException ex) {
            LOG.warn(ex);
        }
    }
}
