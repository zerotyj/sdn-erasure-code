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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Mapper，主要编码工作都在这里
public class EncodeMapper extends Mapper<ECMapReduce.MRInputKey, ECMapReduce.MRInputValue, Text, IntWritable> {

    private Configuration conf;
    private Path ecDir;
    private boolean isDirectory;
    private Path outputDir;
    private FileSystem fileSystem;
    private long blockSize;
    private int parityReplications;
    ErasureCode ec;
    private byte[] zeroBuffer;
    private int bufferSize = 64 * 1024;
    private long timeStamp;
    private List<Path> parityFiles;
    private FSDataInputStream[] inputs = null;
    private FSDataOutputStream[] outputs = null;
    private boolean codeDist = false;
    
    private RaidNodeProtocol client;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
        conf = context.getConfiguration();
        ecDir = new Path(ECMapReduce.MRInputFormat.getInputPath(conf));
        isDirectory = ECMapReduce.MRInputFormat.isDir(conf);
        outputDir = FileOutputFormat.getOutputPath(context);
        fileSystem = FileSystem.get(conf);
        blockSize = ECMapReduce.MRInputFormat.getBlockSize(conf);
        ec = RaidConfig.getErasureCode(conf);
        parityFiles = new ArrayList<Path>(ec.parityCount());
        parityReplications = ECMapReduce.MRInputFormat.getParityReplications(conf);
        codeDist = ECMapReduce.MRInputFormat.getCodeDistirbution(conf);
        
        if(parityReplications == 1) {
            client = RaidNode.getRaidNodeClient(conf);
        }
    }

    // 对每个数据块打开一个输入流
    private FSDataInputStream[] getInputStreams(List<BlockInfo> inblocks) throws IOException {
        FSDataInputStream[] inputs = new FSDataInputStream[ec.stripeCount()];
        for (int i = 0; i < ec.stripeCount(); i++) {
            BlockInfo b = inblocks.get(i);
            if (b.equals(BlockInfo.EMPTY_BLOCK_INFO)) {
                inputs[i] = null;
            } else {
                Path path = new Path(ecDir, b.getPath());
                int blockIndex = b.getBlockIndex();
                
                inputs[i] = fileSystem.open(path);
                inputs[i].seek(blockSize * blockIndex);
            }
        }
        
        return inputs;
    }

    private void closeInputStreams() {
        if (inputs != null) {
            for (FSDataInputStream in : inputs) {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException ex) {

                    }
                }
            }
            inputs = null;
        }
    }

    private void closeOutputStreams() {
        if (outputs != null) {
            for (FSDataOutputStream out : outputs) {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException ex) {

                    }
                }
            }
            outputs = null;
        }
    }

    // 对每个编码块打开一个输出流
    private FSDataOutputStream[] getOutputStreams(int stripeIndex) throws IOException {
        int pc = ec.parityCount();
        FSDataOutputStream[] outputs = new FSDataOutputStream[pc];
        parityFiles.clear();
        if(parityReplications == 1 && codeDist) {
            String[] preferHost = client.getCodeBlockPreferHost(ecDir.toString(), stripeIndex, pc);
            DistributedFileSystem dfs = (DistributedFileSystem)fileSystem;
            if(preferHost != null && preferHost.length == pc) {
                InetSocketAddress[] favoredNodes = new InetSocketAddress[1];
                String dfsaddr = conf.get("dfs.datanode.address", "0.0.0.0:50010");
                int port = Integer.valueOf(dfsaddr.substring(dfsaddr.indexOf(":") + 1));
                for(int i = 0; i < pc; i++) {
                    Path p = new Path(outputDir, RaidConfig.getParityPartFileTempName(stripeIndex, i, timeStamp));
                    favoredNodes[0] = new InetSocketAddress(preferHost[i], port);
                    outputs[i] = dfs.create(p, 
                            FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(conf)), 
                            true, bufferSize, (short)1, blockSize, null, favoredNodes);
                    parityFiles.add(p);
                }
                return outputs;
            }
            
        }
        for (int i = 0; i < pc; i++) {
            Path p = new Path(outputDir, RaidConfig.getParityPartFileTempName(stripeIndex, i, timeStamp));
            outputs[i] = fileSystem.create(p, true, bufferSize, (short) parityReplications, blockSize);
            parityFiles.add(p);
        }
        return outputs;
    }

    private byte[] getZeroBuffer() {
        if (zeroBuffer == null) {
            zeroBuffer = new byte[bufferSize];
            Arrays.fill(zeroBuffer, (byte) 0x00);
        }
        return zeroBuffer;
    }

    // 如果某个块没有数据，则全部用 0 补充
    // 读取数据，如果读到末尾，则用 0 补充
    private void getDataFromInputStream(FSDataInputStream[] inputs, byte[][] buffers, int index) throws IOException {
        if (inputs[index] == null) {
            buffers[index] = getZeroBuffer();
            return;
        }
        if (buffers[index] == null) {
            buffers[index] = new byte[bufferSize];
        }
        int haveRead = inputs[index].read(buffers[index]);
        if (haveRead < buffers[index].length) {
            inputs[index].close();
            inputs[index] = null;
            Arrays.fill(buffers[index], haveRead, buffers[index].length, (byte) 0x00);
        }
    }

    private void doEncode(FSDataInputStream[] inputs, FSDataOutputStream[] output) throws IOException {
        byte[][] parityBuffers = new byte[ec.parityCount()][];
        byte[][] stripeBuffers = new byte[ec.stripeCount()][];
        int bufferSize = 64 * 1024;
        for (int i = 0; i < parityBuffers.length; i++) {
            parityBuffers[i] = new byte[bufferSize];
        }
        long remain = blockSize;
        while (remain > 0) {
            // 读取数据块
            for (int i = 0; i < ec.stripeCount(); i++) {
                getDataFromInputStream(inputs, stripeBuffers, i);
            }
            // 编码
            ec.encode(stripeBuffers, parityBuffers, bufferSize);
            //写入
            for (int i = 0; i < ec.parityCount(); i++) {
                output[i].write(parityBuffers[i]);
            }
            remain -= bufferSize;
        }
    }

    private boolean checkOutputFile(int stripeIndex) throws IOException {
        Path p = new Path(outputDir, RaidConfig.getParityFileName(stripeIndex));
        return fileSystem.exists(p);
    }

    @Override
    public void map(ECMapReduce.MRInputKey key, ECMapReduce.MRInputValue value, Mapper.Context context) throws IOException, InterruptedException {
        //Path outputPath = FileOutputFormat.getOutputPath(context);
        ECMapReduce.LOG.info(String.format("Encode %s : %s to %s", key.toString(), value.toString(), outputDir.toString()));
        List<BlockInfo> blockList = value.getBlockList();
        if (checkOutputFile(key.getStripeIndex())) {
            ECMapReduce.LOG.info(String.format("Stripe %d is done", key.getStripeIndex()));
            return;
        }
        timeStamp = System.currentTimeMillis();
        // 获取多个输入流
        inputs = getInputStreams(blockList);
        // 打开输出文件
        outputs = getOutputStreams(key.getStripeIndex());
        // 进行编码
        doEncode(inputs, outputs);

        // 关闭输入输出流
        closeOutputStreams();
        closeInputStreams();

        doMerge(key.getStripeIndex(), parityFiles);
    }

    @Override
    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        closeOutputStreams();
        closeInputStreams();
    }

    private void doMerge(int stripeIndex, List<Path> parityFiles) throws IOException {
        Path temp = new Path(outputDir, RaidConfig.getParityFileTempName(stripeIndex, timeStamp));
        FSDataOutputStream outTemp = fileSystem.create(temp, (short) 1);
        outTemp.close();
        
        Path[] pathArray = new Path[1];
        for(Path p : parityFiles) {
            pathArray[0] = p;
            fileSystem.concat(temp, pathArray);
        }
        
        Path finalPath = new Path(outputDir, RaidConfig.getParityFileName(stripeIndex));
        fileSystem.rename(temp, finalPath);
    }

}
