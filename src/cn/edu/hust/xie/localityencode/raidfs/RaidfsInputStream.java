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
package cn.edu.hust.xie.localityencode.raidfs;

import cn.edu.hust.xie.localityencode.main.ec.ErasureCode;
import cn.edu.hust.xie.localityencode.main.ec.TooManyErasedLocations;
import cn.edu.hust.xie.localityencode.mr.BlockInfo;
import cn.edu.hust.xie.localityencode.mr.BlockStripeInfo;
import cn.edu.hust.xie.localityencode.mr.RaidConfig;
import cn.edu.hust.xie.localityencode.mr.RaidNode;
import cn.edu.hust.xie.localityencode.mr.RaidNodeProtocol;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.ipc.RPC;

/**
 *
 * @author padicao
 */
public class RaidfsInputStream extends FSInputStream {

    private static final Log LOG = LogFactory.getLog(RaidfsInputStream.class);

    private byte[] onebyte;
    private int bufferSize;
    private FileSystem fileSystem;
    private FSDataInputStream hdfsInput;
    private Path path;

    private boolean useEC;
    private DegradedBlock degradedBlock;
    private boolean simulate;

    private class DegradedBlock {

        private byte[] buffer;
        private int blockIndex;
        private long blockSize;
        private long actualBlockSize;
        private long blockOffset;
        private int bufferOffset;
        private int bufferLength;
        private int recoverLoc;
        private int[] erasedLocs;
        private int[] needLocs;
        private boolean isDirectory;
        private FSDataInputStream[] ecInputs;
        private boolean[] isRemain;
        private ErasureCode ec;
        private List<BlockInfo> blocks;
        
        private byte[][] allBuffers;
        private byte[][] codeBuffers;

        private DegradedBlock(long pos) throws IOException {
            FileStatus status = fileSystem.getFileStatus(path);
            long fileSize = status.getLen();
            blockSize = status.getBlockSize();
            blockIndex = (int) (pos / blockSize);
            blockOffset = pos % blockSize;
            actualBlockSize = Math.min(blockSize, fileSize - pos);
            bufferOffset = 0;
            bufferLength = 0;
            ec = RaidConfig.getErasureCode(fileSystem.getConf());
            
            int sc = ec.stripeCount();
            int pc = ec.parityCount();
            allBuffers = new byte[sc + pc][];
            for(int i = 0; i < allBuffers.length; i++) {
                allBuffers[i] = new byte[bufferSize];
            }
            codeBuffers = new byte[pc][];
            for(int i = 0; i < pc; i++) {
                codeBuffers[i] = allBuffers[i + sc];
            }
            
            LOG.info(String.format("block index %d, offset %d, size %d", blockIndex, blockOffset, actualBlockSize));
        }

        private long getPosition() {
            return blockIndex * blockSize + blockOffset - bufferLength + bufferOffset;
        }

        private boolean reachFileEnd() {
            return actualBlockSize != blockSize;
        }

        private void addErrorIndex(int errIndex) throws TooManyErasedLocations {
            if (erasedLocs == null) {
                erasedLocs = new int[1];
            } else {
                int[] newArr = Arrays.copyOf(erasedLocs, erasedLocs.length + 1);
                erasedLocs = newArr;
            }
            erasedLocs[erasedLocs.length - 1] = errIndex;

            needLocs = ec.locationsToReadForDecode(erasedLocs);
        }

        private void adjustInputStreams() throws IOException {
            for (int loc : needLocs) {
                LOG.info("Need " + loc);
                BlockInfo b = blocks.get(loc);
                long pos = b.getBlockIndex() * blockSize + blockOffset;
                // 对于已经打开的流，重新设置位置
                if (ecInputs[loc] != null) {
                    try {
                        ecInputs[loc].seek(pos);
                        isRemain[loc] = true;
                    } catch (IOException ex) {
                        isRemain[loc] = false;
                    }
                    continue;
                }

                // 打开流
                if (loc < ec.stripeCount()) {
                    // 数据块
                    if (b.equals(BlockInfo.EMPTY_BLOCK_INFO)) {
                        // 空块
                        LOG.info("find empty block at " + loc);
                        ecInputs[loc] = null;
                        isRemain[loc] = false;
                    } else {
                        // 获取输入流，对文件编码和对目录编码，获取方式不一致
                        Path dataPath;
                        if (!isDirectory) {
                            dataPath = path;
                        } else {
                            dataPath = new Path(path.getParent(), b.getPath());
                        }
                        LOG.info(String.format("open file %s, set pos at %d", dataPath.toString(), pos));
                        ecInputs[loc] = fileSystem.open(dataPath);
                        try {
                            ecInputs[loc].seek(pos);
                            isRemain[loc] = true;
                        } catch (IOException ex) {
                            isRemain[loc] = false;
                        }
                    }
                } else {
                    // 编码块
                    Path parityPath;
                    if (isDirectory) {
                        parityPath = RaidConfig.getParityPath(path.getParent());
                    } else {
                        parityPath = RaidConfig.getParityPath(path);
                    }
                    Path parityFilePath = new Path(parityPath, RaidConfig.getParityFileFinalName());
                    LOG.info(String.format("open file %s, set pos at %d", parityFilePath.toString(), pos));
                    ecInputs[loc] = fileSystem.open(parityFilePath);
                    ecInputs[loc].seek(pos);
                    isRemain[loc] = true;
                }
            }
        }

        private boolean doConnect() throws IOException {
            RaidNodeProtocol client = RaidNode.getRaidNodeClient(fileSystem.getConf());
            BlockStripeInfo blockStripeInfo = client.getBlockStripeInfo(path.toString(), blockIndex);
            RPC.stopProxy(client);
            if (blockStripeInfo == null) {
                LOG.info("Block stripe info is null");
                return false;
            }
            LOG.info("Path " + path.getName());
            LOG.info("Block stripe info " + blockStripeInfo.toString());
            blocks = blockStripeInfo.getBlocks();
            
            // Add parity blocks
            for(int i = 0; i < ec.parityCount(); i++) {
                blocks.add(new BlockInfo("", blockStripeInfo.getStripeIndex() * ec.parityCount() + i));
            }
            
            LOG.info("Block stripe info " + blockStripeInfo.toString());

            // 找到出错的块在条带中的位置 
            recoverLoc = 0;
            for (BlockInfo b : blocks) {
                if (b.getBlockIndex() == blockIndex && b.getPath().equals(path.getName())) {
                    break;
                }
                recoverLoc++;
            }

            if(buffer == null)
                buffer = allBuffers[recoverLoc];

            // 从 BlockStripeInfo 中建立多个 InputStream
            ecInputs = new FSDataInputStream[ec.parityCount() + ec.stripeCount()];
            Arrays.fill(ecInputs, null);
            isRemain = new boolean[ec.parityCount() + ec.stripeCount()];
            Arrays.fill(isRemain, false);
            isDirectory = !blocks.get(0).getPath().equals("");

            addErrorIndex(recoverLoc);
            adjustInputStreams();

            LOG.info("Try to recover Location " + recoverLoc);
            return true;
        }

        private void getDataFromInputStream(byte[][] buffers, int index, int len) throws IOException {
            if (!isRemain[index]) {
                Arrays.fill(buffers[index], (byte)0x00);
                return;
            }
	    
            int off = 0;
            while(off < len) {
                int haveRead = ecInputs[index].read(buffers[index], off, len - off);
                LOG.info(String.format("%d read %d, %d", index, len, haveRead));
                if(haveRead < 0) {
                    isRemain[index] = false;
                    Arrays.fill(buffers[index], off, len, (byte)0x00);
                    break;
	        }
                off = off + haveRead;
            }
            //int haveRead = ecInputs[index].read(buffers[index], 0, len);
            //LOG.info(String.format("%d read %d, %d", index, len, haveRead));
            //for (int j = 0; j < 10; j++) {
            //   System.out.print((buffers[index][j] & 0xff) + " ");
            //}
            //System.out.println("");
            //if (haveRead < len) {
            //    isRemain[index] = false;
            //    Arrays.fill(buffers[index], haveRead, len, (byte) 0x00);
            //}
        }

        private boolean doSeek(long pos) throws IOException {
            // 检查是不是在这个块内
            long bindex = pos / blockSize;
            if (bindex != blockIndex) {
                LOG.info("Need to seek out of block");
                return false;
            }

            // 检查是不是在 buffer 内
            long boffset = pos - bindex * blockSize;
            if (boffset >= actualBlockSize) {
                // 超出实际块大小，一般到达文件末尾
                return false;
            }
            long distance = blockOffset - boffset;
            if (distance > 0 && distance <= bufferLength) {
                // 在 buffer 内，使用 buffer
                LOG.info("seek in buffer " + distance);
                bufferOffset = bufferLength - (int) distance;
            } else {
                // 不在 buffer 内，清空 buffer
                LOG.info("seek out of buffer but in block " + boffset);
                blockOffset = boffset;
                bufferOffset = 0;
                bufferLength = 0;
                for (int i = 0; i < ecInputs.length; i++) {
                    if (ecInputs[i] != null) {
                        try {
                            BlockInfo b = blocks.get(i);
                            long offset = b.getBlockIndex() * blockSize + blockOffset;
                            ecInputs[i].seek(offset);
                            isRemain[i] = true;
                            LOG.info(String.format("%d seek to %d", i, offset));
                        } catch (IOException ex) {
                            // 有些块可能没那么多数据
                            isRemain[i] = false;
                        }
                    }
                }
            }
            return true;
        }

        private void readAllNeededDatas(byte[][] readBuffers, int length) throws IOException {
            for(int loc : needLocs) {
                try {
                    getDataFromInputStream(readBuffers, loc, length);
                } catch (BlockMissingException | ChecksumException ex) {
                    addErrorIndex(loc);
                    adjustInputStreams();
                    
                    readAllNeededDatas(readBuffers, length);
                    break;
                }
            }
        }
        private boolean doDecode() throws IOException {
            if (blockOffset == actualBlockSize) {
                // 读到块尾，关闭
                return false;
            }
            //LOG.info(String.format("decode data at %d, %d", blockOffset, bufferSize));
 
            // 读取数据块
            int toRead = (int) Math.min(bufferSize, actualBlockSize - blockOffset);
            readAllNeededDatas(allBuffers, toRead);
            
            ec.decode(allBuffers, codeBuffers, toRead, erasedLocs);
            blockOffset += toRead;
            bufferOffset = 0;
            bufferLength = toRead;
            return true;
        }

        private int doRead(byte[] buf, int off, int len) throws IOException {
            //LOG.info(String.format("DO read %d:%d %d:%d", off, len, bufferOffset, bufferLength));
            int remain = len;
            while (remain > 0) {
                if (bufferOffset >= bufferLength) {
                    if (!doDecode()) {
                        break;
                    }
                }
                int min = Math.min(len, bufferLength - bufferOffset);
                System.arraycopy(buffer, bufferOffset, buf, off, min);
                off += min;
                remain -= min;
                bufferOffset += min;
            }
            if (remain == len) {
                return -1;
            }
            return len - remain;
        }

        private void close() throws IOException {
            if (ecInputs != null) {
                for (FSDataInputStream in : ecInputs) {
                    if (in != null) {
                        in.close();
                    }
                }
                ecInputs = null;
            }
        }
    }

    public RaidfsInputStream(FileSystem fs, Path hdfsPath, int bufferSize) throws IOException {
        onebyte = new byte[1];
        this.useEC = false;
        this.fileSystem = fs;
        this.path = hdfsPath;
        hdfsInput = fs.open(hdfsPath, bufferSize);
        this.bufferSize = bufferSize;
        this.degradedBlock = null;
        this.simulate = false;
    }                    

    public void setSimulate(boolean s) {
        this.simulate = s;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (useEC) {
            // 相应处理
            if (!degradedBlock.doSeek(pos)) {
                // 跳到块外，关闭，使用原来的 hdfsInput
                degradedBlock.close();
                degradedBlock = null;
                useEC = false;
                hdfsInput.seek(pos);
            }
        } else {
            hdfsInput.seek(pos);
        }
    }

    @Override
    public long getPos() throws IOException {
        if (useEC) {
            return degradedBlock.getPosition();
        } else {
            return hdfsInput.getPos();
        }
    }

    // 不支持从别的节点读取
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        int ret = read(onebyte, 0, 1);
        if (ret != -1) {
            return -1;
        }
        return onebyte[0] & 0xff;
    }

    private boolean openECInputStream(long offset) throws IOException {
        LOG.info("open ECInputStream at " + offset);
        degradedBlock = new DegradedBlock(offset);
        return degradedBlock.doConnect();
    }

    @Override
    public int read(byte[] buf, int offset, int length) throws IOException {
        if (useEC) {
            int haveRead = degradedBlock.doRead(buf, offset, length);
            if (haveRead == -1) {
                // 判断是该块读完，还是文件读完
                boolean reachFileEnd = degradedBlock.reachFileEnd();
                long position = degradedBlock.getPosition();
                degradedBlock.close();
                degradedBlock = null;
                useEC = false;
                if (!reachFileEnd) {
                    // 开启新块，从 hdfsInput中获取数据
                    hdfsInput.seek(position);
                    return read(buf, offset, length);
                }
            }
            return haveRead;
        } else {
            long pos = hdfsInput.getPos();
            try {
                if (simulate) {
                    throw new BlockMissingException(path.toString(), "Simulate Block mission exception", pos);
                }
                return hdfsInput.read(buf, offset, length);
            } catch (BlockMissingException | ChecksumException ex) {
                useEC = true;
                if (!openECInputStream(pos)) {
                    LOG.info("No raid for this block");
                    throw ex;
                }
                return read(buf, offset, length);
            }
        }
    }

    @Override
    public int read(byte[] buf) throws IOException {
        return read(buf, 0, buf.length);
    }

}
