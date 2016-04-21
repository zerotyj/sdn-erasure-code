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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.ExportUtils;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockRackMap;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementProtocol;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementServer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author padicao
 */
public abstract class ECMapReduce {

    public static final Log LOG = LogFactory.getLog(ECMapReduce.class);

    protected FileSystem fs;
    protected Configuration conf;
    protected ErasureCode erasureCode;
    protected boolean isDirectory;    // 是否按目录进行编码
    protected long blockSize;
    protected List<FileStatus> encodingFiles;
    protected List<TaskInfo> tasks;        // 任务，可以按节点排序
    protected RaidNode node;

    // BlockID => PreferRack
    protected Map<Long, String> preferRackMap;
    private BlockPlacementProtocol blockClient;
    private boolean needSetReplicas;

    protected Path dataPath;            // 编码的数据目录
    protected Path parityPath;           //  编码后的编码块目录   
    protected Path metaDataPath;    // 编码所用的保存条带信息，用于 MapReduce的输入
    protected Path mrPath;                // MapReduce 使用的目录
    protected Path finalParityPath;     // 最后的编码文件
    protected int parityReplication;      // 编码块的副本数据
    protected boolean codeDist;           // 编码块的布局

    public abstract void selectTask() throws IOException;

    public void setMRInputFormat(Job job) {
        job.setInputFormatClass(MRInputFormat.class);
    }

    public MRInputFormat getMRInputFormat() {
        return new MRInputFormat();
    }

    protected void initialize(RaidNode node, String path) throws IOException {
        this.node = node;
        conf = node.getConf();
        //fc = node.getFileContext();
        fs = node.getFileSystem();
        tasks = new ArrayList<TaskInfo>();

        this.dataPath = fs.makeQualified(new Path(path));
        this.parityPath = RaidConfig.getParityPath(dataPath);
        this.metaDataPath = new Path(parityPath, RaidConfig.getMetaDataName());
        this.mrPath = new Path(parityPath, RaidConfig.getMRPathName());
        this.finalParityPath = new Path(parityPath, RaidConfig.getParityFileFinalName());
        this.erasureCode = RaidConfig.getErasureCode(conf);
        this.preferRackMap = new HashMap<Long, String>();
        this.needSetReplicas = conf.getBoolean(RaidConfig.SET_REPLICAS, true);
        this.codeDist = false;

        FileStatus fileStatus = fs.getFileStatus(dataPath);
        this.isDirectory = fileStatus.isDirectory();
        encodingFiles = new ArrayList<FileStatus>();
        blockSize = -1;
        if (isDirectory) {
            FileStatus[] files = fs.listStatus(dataPath);
            for (FileStatus f : files) {
                if (blockSize == -1) {
                    blockSize = f.getBlockSize();
                } else if (blockSize != f.getBlockSize()) {
                    throw new IOException(String.format("%s has different block size %d compare to %d",
                            f.getPath().toString(), f.getBlockSize(), blockSize));
                }
                encodingFiles.add(f);
            }
        } else {
            encodingFiles.add(fileStatus);
            blockSize = fileStatus.getBlockSize();
        }
    }

    protected void addTask(TaskInfo t) {
        tasks.add(t);
    }

    protected void setParityReplication(int r) {
        this.parityReplication = r;
    }
    
    protected void setCodeDistirubtion(boolean b) {
        this.codeDist = b;
    }

    protected void setPreferRack(String filepath, String[] preferRacks) throws IOException {
        String file = Path.getPathWithoutSchemeAndAuthority(new Path(filepath)).toString();
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        long[] blockIDs = ExportUtils.getBlockIDs(dfs, file, blockSize * preferRacks.length);
        for (int i = 0; i < blockIDs.length; i++) {
            preferRackMap.put(blockIDs[i], preferRacks[i]);
        }
    }

    private void informNamenodePreferRackMap() throws IOException {
        BlockRackMap map = new BlockRackMap(preferRackMap);
        if (blockClient == null) {
            blockClient = BlockPlacementServer.getBlockPlacementClient(conf);
        }
        blockClient.setBlockPreferRacks(map);
    }

    protected void adjustReplication() throws IOException {
        if (this.needSetReplicas) {
            if (!preferRackMap.isEmpty()) {
                LOG.info("Inform namenode prefer hosts");
                informNamenodePreferRackMap();
            }
            if (parityReplication > 1) {
                LOG.info("Set parity file replication to 1");
                fs.setReplication(finalParityPath, (short) 1);
            }
            LOG.info("Set data file replication to 1");
            for (FileStatus file : encodingFiles) {
                fs.setReplication(file.getPath(), (short) 1);
            }
        }
    }

    public void doRaid(RaidNode node, String path) throws Exception {
        // 初始化
        initialize(node, path);
        // 选择数据块，形成条带
        selectTask();
        // 排序，按节点排序
        Collections.sort(tasks);
        // 将任务打印输出，以供调试
        outputTaskInfos();
        // 将任务写入 HDFS，以运行 MapReduce
        writeTask();
        // 执行任务
        if (!executeJob()) {
            return;
        }
        // 清理任务，并将多个文件合并成一个
        afterJob();
        // 将副本数目设置为 1
        adjustReplication();
    }

    private void checkParityDist(FileSystem fs, Path p, long filesize) throws IOException {
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(p, 0, filesize);
        for (BlockLocation b : fileBlockLocations) {
            LOG.info(b.toString());
        }
    }

    protected void afterJob() throws IOException {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        Path finalTemp = new Path(mrPath, RaidConfig.getParityFileFinalTempName(System.currentTimeMillis()));
        FSDataOutputStream outTemp = dfs.create(finalTemp, (short) parityReplication);
        outTemp.close();

        int stripeCount = tasks.size();
        LOG.info(String.format("Begin to merge %d files", stripeCount));
        Path[] pathes = new Path[1];
        for (int i = 0; i < stripeCount; i++) {
            //for (int j = 0; j < erasureCode.paritySize(); j++) {
            pathes[0] = new Path(mrPath, RaidConfig.getParityFileName(i));
            dfs.concat(finalTemp, pathes);
            //}
        }

        LOG.info(String.format("Finishmerge %d files", stripeCount));
        //checkParityDist(dfs, finalTemp, stripeCount * erasureCode.parityCount() * blockSize);

        LOG.info(String.format("Rename %s to %s", finalTemp, finalParityPath));
        dfs.rename(finalTemp, finalParityPath);

        LOG.info("Delete MapReudce output directory " + mrPath.toString());
        fs.delete(mrPath, true);
    }

    private void outputTaskInfos() {
        System.out.println("Number of Tasks : " + tasks.size());
        for (TaskInfo t : tasks) {
            System.out.println(t.toString());
        }
    }

    // 将 MapReduce 作业的输入数据写入 HDFS
    private void writeTask() throws IOException {
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(this.metaDataPath),
                SequenceFile.Writer.keyClass(MRInputKey.class), SequenceFile.Writer.valueClass(MRInputValue.class));
        MRInputKey key = new MRInputKey();
        MRInputValue value = new MRInputValue();
        for (TaskInfo t : tasks) {
            key.setHost(t.getHost());
            key.setStripeIndex(t.getStripeIndex());
            value.setBlockList(t.getBlocks());
            writer.append(key, value);
        }
        writer.close();
    }

    // 生成作业，并执行
    private boolean executeJob() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "Local Encode");
        job.setJarByClass(PSWOECMapReduce.class);
        job.setMapperClass(EncodeMapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(0);
        setMRInputFormat(job);
        //job.setOutputFormatClass(FileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        MRInputFormat.setInputPath(job, dataPath.toString());
        MRInputFormat.setMetaPath(job, metaDataPath.toString());
        MRInputFormat.setIsDirectory(job, isDirectory);
        MRInputFormat.setBlockSize(job, blockSize);
        //MRInputFormat.setTasksPerMap(job, 5);
        MRInputFormat.setParityReplications(job, parityReplication);
        MRInputFormat.setCodeDistirbution(job, codeDist);

        // It can be commentted
        job.getConfiguration().setBoolean("mapreduce.map.speculative", false);

        FileOutputFormat.setOutputPath(job,
                this.mrPath);

        //-------------------------------------
        //testReader(job);
        //------------------------------------
        boolean status = job.waitForCompletion(true);
        LOG.info("job finish " + (status ? "successfully" : "fail"));
        return status;
    }

    // 读取，貌似不能正常使用 reader.sync(position)，使用reader.seek(position)
    private void testReader(Job job) throws IOException, InterruptedException {
        MRInputFormat inputFormat = getMRInputFormat();
        List<InputSplit> splits = inputFormat.getSplits(job);
        MRInputKey key = new MRInputKey();
        MRInputValue value = new MRInputValue();
        for (InputSplit split : splits) {
            FileSplit fileSplit = (FileSplit) split;
            LOG.info(fileSplit);
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(this.metaDataPath));
            reader.seek(fileSplit.getStart());
            //reader.sync(fileSplit.getStart());
            long pos = reader.getPosition();

            while (pos < fileSplit.getLength() + fileSplit.getStart()) {
                reader.next(key, value);
                LOG.info(String.format("%s : %s", key, value));
                pos = reader.getPosition();
            }
            reader.close();
        }
    }

    // MapReduce 的 Map 输入 Key，主要包含文件/目录，条带索引
    public static class MRInputKey implements Writable {

        private String host;
        private int stripeIndex;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getStripeIndex() {
            return stripeIndex;
        }

        public void setStripeIndex(int stripeIndex) {
            this.stripeIndex = stripeIndex;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(stripeIndex);
            out.writeUTF(host);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            stripeIndex = in.readInt();
            host = in.readUTF();
        }

        @Override
        public String toString() {
            return String.format("%d(%s)", stripeIndex, host);
        }
    }

    // MapReduce 的 Map 输入 Value，主要包含构建该条带的块
    public static class MRInputValue implements Writable {

        private List<BlockInfo> blockList;

        MRInputValue(List<BlockInfo> blocks, Map<String, String> ex) {
            this.blockList = blocks;
        }

        MRInputValue() {

        }

        public void setBlockList(List<BlockInfo> blockList) {
            this.blockList = blockList;
        }

        public List<BlockInfo> getBlockList() {
            return blockList;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(blockList.size());
            for (BlockInfo b : blockList) {
                b.write(out);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int len = in.readInt();
            blockList = new ArrayList<BlockInfo>(len);
            for (int i = 0; i < len; i++) {
                BlockInfo b = new BlockInfo();
                b.readFields(in);
                blockList.add(b);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("{ ");
            for (BlockInfo b : blockList) {
                sb.append(b.toString()).append(" ");
            }
            sb.append("}");
            return sb.toString();
        }
    }

    // MapReduce 的 InputFormat，每个条带的编码便是一个任务
    public static class MRInputFormat extends InputFormat<MRInputKey, MRInputValue> {

        private static String INPUT_PATH = "mapreduce.localecode.input.path";
        private static String META_PATH = "mapreduce.localencode.input.meta.path";
        private static String IS_DIR = "mapreduce.localencode.input.isdir";
        private static String BLOCK_SIZE = "mapreduce.localencode.input.blocksize";
        private static String TASKS_PER_MAP = "mapreduce.localencode.input.taskspermap";
        private static String PARITY_REPLICATIONS = "mapreduce.localencode.input.parityreplications";
        private static String CODE_DIST = "mapreduce.localencode.input.codedist";

        // 一些配置
        public static void setMetaPath(Job job, String path) {
            job.getConfiguration().set(META_PATH, path);
        }

        public static void setBlockSize(Job job, long blocksize) {
            job.getConfiguration().setLong(BLOCK_SIZE, blocksize);
        }

        public static void setInputPath(Job job, String path) {
            job.getConfiguration().set(INPUT_PATH, path);
        }

        public static void setIsDirectory(Job job, boolean isDir) {
            job.getConfiguration().setBoolean(IS_DIR, isDir);
        }

        public static void setParityReplications(Job job, int r) {
            job.getConfiguration().setInt(PARITY_REPLICATIONS, r);
        }
        
        public static void setCodeDistirbution(Job job, boolean b) {
            job.getConfiguration().setBoolean(CODE_DIST, b);
        }

        public static String getMetaPath(Configuration conf) {
            return conf.get(META_PATH);
        }

        public static long getBlockSize(Configuration conf) {
            return conf.getLong(BLOCK_SIZE, 0L);
        }

        public static String getInputPath(Configuration conf) {
            return conf.get(INPUT_PATH);
        }

        public static boolean isDir(Configuration conf) {
            return conf.getBoolean(IS_DIR, false);
        }

        public static void setTasksPerMap(Job job, int count) {
            job.getConfiguration().setInt(TASKS_PER_MAP, count);
        }

        public static int getTasksPerMap(Configuration conf) {
            return conf.getInt(TASKS_PER_MAP, 5);
        }

        public static int getParityReplications(Configuration conf) {
            return conf.getInt(PARITY_REPLICATIONS, 3);
        }
        
        public static boolean getCodeDistirbution(Configuration conf) {
            return conf.getBoolean(CODE_DIST, false);
        }

        private static FileSplit newFileSplit(Path path, long start, long length, String host) {
            String[] hosts;
            if ("".equals(host)) {
                hosts = new String[0];
            } else {
                hosts = new String[1];
                hosts[0] = host;
            }
            FileSplit s = new FileSplit(path, start, length, hosts);
            LOG.info(s.toString());
            return s;
        }

        // 划分任务
        @Override
        public List<InputSplit> getSplits(JobContext context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path path = new Path(getMetaPath(context.getConfiguration()));
            SequenceFile.Reader reader = null;
            try {
                reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
                ECMapReduce.MRInputKey key = new ECMapReduce.MRInputKey();
                MRInputValue value = new MRInputValue();
                long current = 0L;
                long prev = reader.getPosition();
                List<InputSplit> results = new ArrayList<InputSplit>();
                String prevHost = null;
                int targetCount = getTasksPerMap(conf);
                int count = 0;
                while (reader.next(key, value)) {
                    String curHost = key.getHost();
                    if (prevHost == null) {
                        prevHost = curHost;
                    } else {
                        // 同一节点的任务合成一个
                        if (count > 0 && !curHost.equals(prevHost)) {
                            FileSplit s = newFileSplit(path, prev, current - prev, prevHost);
                            results.add(s);

                            count = 0;
                            prevHost = curHost;
                            prev = current;
                        }
                    }
                    current = reader.getPosition();
                    count++;
                    if (count == targetCount) {
                        FileSplit s = newFileSplit(path, prev, current - prev, prevHost);
                        results.add(s);
                        prevHost = null;
                        prev = current;
                        count = 0;
                    }
                }
                if (current > prev) {
                    FileSplit s = newFileSplit(path, prev, current - prev, prevHost);
                    results.add(s);
                }
                return results;
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        }

        // Hadoop 提供的 SequenceFileRecordReader 貌似不能正常使用，这里简单实现一下
        @Override
        public RecordReader<MRInputKey, MRInputValue> createRecordReader(InputSplit split,
                TaskAttemptContext context) throws IOException, InterruptedException {
            RecordReader<MRInputKey, MRInputValue> reader = new RecordReader() {
                private SequenceFile.Reader reader;
                private long pos;
                private long end;
                private long start;
                private MRInputKey key;
                private MRInputValue value;

                @Override
                public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                    String path = getMetaPath(context.getConfiguration());
                    reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(new Path(path)));
                    FileSplit fSplit = (FileSplit) split;
                    start = fSplit.getStart();
                    end = start + fSplit.getLength();
                    reader.seek(start);
                    pos = reader.getPosition();
                    key = new MRInputKey();
                    value = new MRInputValue();
                }

                @Override
                public boolean nextKeyValue() throws IOException, InterruptedException {
                    pos = reader.getPosition();
                    if (pos >= end) {
                        return false;
                    }

                    return reader.next(key, value);
                }

                @Override
                public Object getCurrentKey() throws IOException, InterruptedException {
                    return key;
                }

                @Override
                public Object getCurrentValue() throws IOException, InterruptedException {
                    return value;
                }

                @Override
                public float getProgress() throws IOException, InterruptedException {
                    return 1 - (end - pos) / (float) (end - start);
                }

                @Override
                public void close() throws IOException {
                    reader.close();
                }
            };
            reader.initialize(split, context);
            return reader;
        }

    }

}
