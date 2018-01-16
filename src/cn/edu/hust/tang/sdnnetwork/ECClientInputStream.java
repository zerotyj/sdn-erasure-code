package cn.edu.hust.tang.sdnnetwork;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.ipc.RPC;

import com.sun.jna.Pointer;

import cn.edu.hust.tang.ec.JerasureLibrary;
import cn.edu.hust.tang.packets.AckPacket;
import cn.edu.hust.tang.packets.ClosePacket;
import cn.edu.hust.tang.packets.LinkPacket;
import cn.edu.hust.tang.packets.Packet;
import cn.edu.hust.tang.packets.ServerInfo;
import cn.edu.hust.xie.localityencode.main.ec.ErasureCode;
import cn.edu.hust.xie.localityencode.main.ec.TooManyErasedLocations;
import cn.edu.hust.xie.localityencode.mr.BlockInfo;
import cn.edu.hust.xie.localityencode.mr.BlockStripeInfo;
import cn.edu.hust.xie.localityencode.mr.RaidConfig;
import cn.edu.hust.xie.localityencode.mr.RaidNode;
import cn.edu.hust.xie.localityencode.mr.RaidNodeProtocol;
import restapi.GetBuildTreeRequest;
import restapi.GetBuildTreeResponse;
import restapi.RestClient;
import restapi.SimpleBuildTree;

public class ECClientInputStream extends FSInputStream {
	
	private static final Log LOG = LogFactory.getLog(ECClientInputStream.class);
	
	private int bufferSize;
	private FileSystem fileSystem;
	private Path hdfsPath;
	private FSDataInputStream hdfsInput;
	private FileContext fc;
	private boolean useEC;
	private boolean simulate;
	private boolean isLinked;
	private DegradedBlock degradedBlock;
	
	private String url = "http://10.0.0.2:8080";
	
	
	private static final int PORT = 12345;
	private static final String IP = "10.0.0.26";
	private Socket conSocket = null;
	private DataInputStream input = null;
	private DataOutputStream output = null;
	private ArrayList<ServerInfo> info;
	private Buffer[] buffer = new Buffer[2];
	private WriteThread write;
	
	
	private class DegradedBlock {

        private int[] decodeMatrix;
        private int[] factor;
        private long[] blockPos;
        private String[] blockPath;
        private String[] ipAddr;
        private int blockIndex;
        private long blockSize;
        private long actualBlockSize;
        private long blockOffset;
        private int recoverLoc;
        private int[] erasedLocs;
        private int[] needLocs;
        private boolean isDirectory;
        private FSDataInputStream[] ecInputs;
        private boolean[] isRemain;
        private ErasureCode ec;
        private List<BlockInfo> blocks;
        

        private DegradedBlock(long pos) throws IOException {
            FileStatus status = fileSystem.getFileStatus(hdfsPath);
            long fileSize = status.getLen();
            blockSize = status.getBlockSize();
            blockIndex = (int) (pos / blockSize);
            blockOffset = pos % blockSize;
            actualBlockSize = Math.min(blockSize, fileSize - blockSize * blockIndex);
            ec = RaidConfig.getErasureCode(fileSystem.getConf());
            
            int sc = ec.stripeCount();
            decodeMatrix = new int[sc * sc];
            factor = new int[sc];
            blockPos = new long[sc];
            blockPath = new String[sc];
            ipAddr = new String[sc];
            
            LOG.info(String.format("block index %d, offset %d, size %d", blockIndex, blockOffset, actualBlockSize));
        }
        
        private boolean doConnect() throws IOException {
        	RaidNodeProtocol client = RaidNode.getRaidNodeClient(fileSystem.getConf());
            // get stripe info where degradedblock is in
        	BlockStripeInfo blockStripeInfo = client.getBlockStripeInfo(hdfsPath.toString(), blockIndex);
            RPC.stopProxy(client);
            if (blockStripeInfo == null) {
                LOG.info("Block stripe info is null");
                return false;
            }
            LOG.info("Path " + hdfsPath.getName());
            LOG.info("Block stripe info " + blockStripeInfo.toString());
            // only get data block info
            blocks = blockStripeInfo.getBlocks();
            
            // Add parity blocks
            for(int i = 0; i < ec.parityCount(); i++) {
                blocks.add(new BlockInfo("", blockStripeInfo.getStripeIndex() * ec.parityCount() + i));
            }
            
            LOG.info("Block stripe info " + blockStripeInfo.toString());

            // 找到出错的块在条带中的位置 
            // 文件编码时：只需要块索引即可确定位置
            // 目录编码时：需要块索引和路径两个条件才能确定位置
            recoverLoc = 0;
            for (BlockInfo b : blocks) {
                if (b.getBlockIndex() == blockIndex && b.getPath().equals(hdfsPath.getName())) {
                    break;
                }
                recoverLoc++;
            }

//            if(buffer == null)
//                buffer = allBuffers[recoverLoc];

            // 从 BlockStripeInfo 中建立多个 InputStream
            ecInputs = new FSDataInputStream[ec.parityCount() + ec.stripeCount()];
            Arrays.fill(ecInputs, null);
            isRemain = new boolean[ec.parityCount() + ec.stripeCount()];
            Arrays.fill(isRemain, false);
            isDirectory = !blocks.get(0).getPath().equals("");

            addErrorIndex(recoverLoc);
            adjustInputStreams();
            
            // 应该在这里确定好 needLocs 和 erasedLocs
            getFinalNeededBlock();
            // 确定之后再重新定位流到 pos 处
            adjustInputStreams();
            if (ecInputs != null) {
                for (FSDataInputStream in : ecInputs) {
                    if (in != null) {
                        in.close();
                    }
                }
                ecInputs = null;
            }
            
            /*for (int i = 0; i < needLocs.length; i++) {
            	BlockLocation[] fileBlockLocations = fc.getFileBlockLocations(hdfsPath, 
            			blocks.get(needLocs[i]).getBlockIndex() * blockSize, blockSize);
            	ipAddr[i] = InetAddress.getByName(fileBlockLocations[0].getHosts()[0]).getHostAddress();
            	LOG.info("needLoc " + i + " 's IP is " + ipAddr[i]);
            }*/
	    Path parityPath;
            BlockLocation[] fileBlockLocations;
            if (isDirectory) {
                parityPath = RaidConfig.getParityPath(hdfsPath.getParent());
            } else {
                parityPath = RaidConfig.getParityPath(hdfsPath);
            }
            Path parityFilePath = new Path(parityPath, RaidConfig.getParityFileFinalName());
	    // hdfsPath：数据块  如何获取校验块所在路径
            for (int i = 0; i < needLocs.length; i++) {
            	if (needLocs[i] < ec.stripeCount()) {
            		// 这里的 hdfsPath 需要考虑是文件编码还是目录编码吗
            		fileBlockLocations = fc.getFileBlockLocations(hdfsPath, 
            				blocks.get(needLocs[i]).getBlockIndex() * blockSize, blockSize);
            	} else {
            		fileBlockLocations = fc.getFileBlockLocations(parityFilePath, 
            				blocks.get(needLocs[i]).getBlockIndex() * blockSize, blockSize);
            	}
            	ipAddr[i] = InetAddress.getByName(fileBlockLocations[0].getHosts()[0]).getHostAddress();
            	LOG.info("needLoc " + i + " 's IP is " + ipAddr[i]);
            }
            
            // 将 erasedLocs 数组转换为 erased 数组
            int[] erased = new int[ec.parityCount() + ec.stripeCount()];
            int[] dm_ids = new int[ec.stripeCount()];
            for (int i=0; i < erasedLocs.length; i++) {
        		erased[erasedLocs[i]] = 1;
        	}
            // 根据 erasured 数组和生成矩阵求解码矩阵
            Pointer matrix = JerasureLibrary.INSTANCE.reed_sol_vandermonde_coding_matrix(
            		ec.stripeCount(), ec.parityCount(), 8);
            JerasureLibrary.INSTANCE.jerasure_make_decoding_matrix(
            		ec.stripeCount(), ec.parityCount(), 8, matrix, erased, decodeMatrix, dm_ids);
            // 判断可用块 dm_ids 和 needLocs 是否相同
            if (!isEqual(dm_ids, needLocs)) {
            	return false;
            }
            
            // 确定服务器读取数据后进行 GF 域乘法计算的系数
            for (int i=0; i < ec.stripeCount(); i++) {
            	factor[i] = decodeMatrix[recoverLoc * ec.stripeCount() + i];
            }
            
            LOG.info("Try to recover Location " + recoverLoc);
            return true;
        }
        
        private boolean isEqual(int[] a, int[] b) {
        	if (a.length != b.length) {
        		return false;
        	}
        	for (int i=0; i < a.length; i++) {
        		if (a[i] != b[i]) {
        			return false;
        		}
        	}
        	return true;
        }
        
        private void getFinalNeededBlock() throws IOException {
        	// 应该在这里确定好 needLocs 和 erasedLocs
            // 确定之后再重新定位流到 pos 处
            byte[] oneByte = new byte[1];
            for (int loc : needLocs) {
            	if (!isRemain[loc]) {
                    continue;
                }
            	try {
					ecInputs[loc].read(oneByte, 0, 1);
				} catch (BlockMissingException | ChecksumException ex) {
					addErrorIndex(loc);
                    adjustInputStreams();
                    
                    getFinalNeededBlock();
                    break;
                    // break之后还要重新定位流
				}
            }
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
            
        	for (int i = 0; i < needLocs.length; i++) {
                int loc = needLocs[i];
        		LOG.info("Need " + loc);
                BlockInfo b = blocks.get(loc);
                long pos = b.getBlockIndex() * blockSize + blockOffset;
                blockPos[i] = pos;
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
                            dataPath = hdfsPath;
                        } else {
                            dataPath = new Path(hdfsPath.getParent(), b.getPath());
                        }
                        LOG.info(String.format("open file %s, set pos at %d", dataPath.toString(), pos));
                        blockPath[i] = dataPath.toString();
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
                        parityPath = RaidConfig.getParityPath(hdfsPath.getParent());
                    } else {
                        parityPath = RaidConfig.getParityPath(hdfsPath);
                    }
                    Path parityFilePath = new Path(parityPath, RaidConfig.getParityFileFinalName());
                    LOG.info(String.format("open file %s, set pos at %d", parityFilePath.toString(), pos));
                    blockPath[i] = parityFilePath.toString();
                    ecInputs[loc] = fileSystem.open(parityFilePath);
                    ecInputs[loc].seek(pos);
                    isRemain[loc] = true;
                }
            }
        }
        
        
        private long getPosition() {
            return blockIndex * blockSize + blockOffset;
        }

        private boolean reachFileEnd() {
            return actualBlockSize != blockSize;
        }
        
        private void close() throws IOException {
        	if (write.isAlive()) {
        		write.setClosed(true);
			System.out.println("write is alive");
        		try {
        			System.out.println("ready to enter write");
				write.join();
				System.out.println("leave write");
        			
        		} catch (InterruptedException e) {
        			System.out.println("write thread error:" + e.getMessage());
        		}
        	}
    		System.out.println("ready to send ClosePacket");
		ClosePacket closePkt = new ClosePacket();
    		Packet.writeOutput(output, closePkt);
    		input.close();
    		output.close();
    		conSocket.close();
		System.out.println("read is end");
        }
        
        private void createInfo(SimpleBuildTree tree, ServerInfo infoTree) {
//        	info = new ArrayList<ServerInfo>();
//        	for (int i=0; i < ec.stripeCount(); i++) {
//        		info.add(new ServerInfo(ipAddr[i], blockPath[i], blockPos[i], factor[i]));
//        	}
        	// 如果是叶子节点
        	if (tree.getChildIps() == null) {
        		for (int i = 0; i < info.size(); i++) {
        			ServerInfo s = info.get(i);
        			if (tree.getHostIp().equals(s.getIP())) {
        				infoTree.setInfo(s.getIP(), s.getPath(), s.getPos(), s.getFactor());
        				info.remove(i);
        				break;
        			}
        		}
        		return;
        	}
        	ServerInfo[] child = new ServerInfo[tree.getChildIps().length];
        	infoTree.setChildInfo(child);
        	for (int i = 0; i < tree.getChildIps().length; i++) {
        		infoTree.getChildInfo()[i] = new ServerInfo();
			createInfo(tree.getChildIps()[i], infoTree.getChildInfo()[i]);
        	}
        }
        
        private void outputSimpleBuildTree(SimpleBuildTree root) {
            System.out.println(String.format("%s: %d childs", root.getHostIp(), root.getChildIps() == null ? 0 : root.getChildIps().length));
            if (root.getChildIps() != null) {
                for (SimpleBuildTree c : root.getChildIps()) {
                    outputSimpleBuildTree(c);
                }
            }
        }
        
        private void initialize() {
        	try {
        		// 初始化请求
        		String clientIP = InetAddress.getLocalHost().getHostAddress().toString();
        		LOG.info(String.format("client IP is %s", clientIP));
        		GetBuildTreeRequest req = new GetBuildTreeRequest();
        		req.setClient(clientIP);
        		req.setServers(ipAddr);
        		// 初始化客户端
        		RestClient client = new RestClient(url);
        		// 调用 Rest API
	            GetBuildTreeResponse resp = client.doRestCall("/wm/nc/get/buildtree/json", req, GetBuildTreeResponse.class);
	            SimpleBuildTree tree = resp.getTree();
	            // 获取结果
	            outputSimpleBuildTree(tree);
	            
        		conSocket = new Socket(tree.getChildIps()[0].getHostIp(), PORT);
			info = new ArrayList<ServerInfo>();
            	for (int i=0; i < ec.stripeCount(); i++) {
            		info.add(new ServerInfo(ipAddr[i], blockPath[i], blockPos[i], factor[i]));
            	}
            	ServerInfo infoTree = new ServerInfo();
		createInfo(tree, infoTree);
		System.out.println("create success!");	
            	
            	input = new DataInputStream(conSocket.getInputStream());
        		output = new DataOutputStream(conSocket.getOutputStream());
        		// send link request
        		LinkPacket linkPkt = new LinkPacket(tree.getChildIps()[0], infoTree.getChildInfo()[0], bufferSize);
			Packet.writeOutput(output, linkPkt);
        		// receive AckPacket	
        		Packet pkt = Packet.readInput(input);
        		if (pkt.getType() == Packet.ACK_PACKET_TYPE) {
        			AckPacket ackPkt = (AckPacket) pkt;
        			if (ackPkt.getResult()) {
        				System.out.println("link success");
        			} else {
        				System.out.println("link error: ack is false");
        				throw new IOException("ack is false");
        			}
        		} else {
        			throw new IOException(String.format("wrong packet type: %d", pkt.getType()));
        		}	
        		buffer[0] = new Buffer(bufferSize);
        		buffer[1] = new Buffer(bufferSize);
        		write = new WriteThread(buffer, input, output, blockOffset, actualBlockSize);
        		write.start();
        		LOG.info(String.format("blockOffset %d, actualBlockSize %d", blockOffset, actualBlockSize));
        	} catch (IOException e) {
        		e.printStackTrace();
        	}
        } 
        
        private int SDNRead(byte[] b, int off, int len) throws IOException {
    		int remain = len;
    		int counter = 0;
    		while (remain > 0) {
//    			返回值为-1表示已经没有数据可读
    			int ret = buffer[Buffer.getReadBufferIndex()].readBuffer(b, off, remain);
//    			如果第一次读取就没有数据可读
//    			LOG.info(String.format("have read : %d", ret));
    			if (ret == -1) {
    				if (counter == 0) {
    					return -1;
    				} else {
    					return counter;
    				}
    			}
    			off += ret;
    			counter += ret;
    			remain -= ret;
    			blockOffset += ret;
    		}
    		LOG.info("read end!");
    		return counter;
    	}
	}
		
	public ECClientInputStream(FileSystem fs,
			Path hdfsPath, int buffersize) throws IOException {
		useEC = false;
		simulate = false;
		isLinked = false;
		fileSystem = fs;
		this.hdfsPath = hdfsPath;
		this.bufferSize = buffersize;
		hdfsInput = fs.open(hdfsPath, buffersize);
		fc = FileContext.getFileContext(new Configuration());
		degradedBlock = null;
	}
	
	
	public void setSimulate(boolean s) {
        this.simulate = s;
    }
	
	@Override
	public int read() throws IOException {
		byte[] onebyte = new byte[1];
		int ret = read(onebyte, 0, 1);
		if (ret == -1) {
			return -1;
		}
		return onebyte[0] & 0xff;
	}
	
	
	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (useEC) {
			if (!isLinked) {
				degradedBlock.initialize();
				isLinked = true;
			}
			int haveRead = degradedBlock.SDNRead(b, off, len);
            if (haveRead == -1) {
                // 判断是该块读完，还是文件读完
            	System.out.println("ECC: have read a block!");
                boolean reachFileEnd = degradedBlock.reachFileEnd();
                long position = degradedBlock.getPosition();
                degradedBlock.close();
		System.out.println("ECC: degradedBlock is closed!");
                degradedBlock = null;
                useEC = false;
                isLinked = false;
                if (!reachFileEnd) {
                    // 开启新块，从 hdfsInput中获取数据
                    hdfsInput.seek(position);
                    return read(b, off, len);
                }
            }
            return haveRead;
		} else {
			long pos = hdfsInput.getPos();
            try {
                if (simulate) {
                    throw new BlockMissingException(hdfsPath.toString(), 
                    		"Simulate Block mission exception", pos);
                }
                return hdfsInput.read(b, off, len);
            } catch (BlockMissingException | ChecksumException ex) {
                useEC = true;
                if (!openECInputStream(pos)) {
                    LOG.info("No raid for this block");
                    throw ex;
                }
                return read(b, off, len);
            }
		}	
	}
	
	private boolean openECInputStream(long pos) throws IOException {
		LOG.info("open ECInputStream at " + pos);
        degradedBlock = new DegradedBlock(pos);
        return degradedBlock.doConnect();
	}

	@Override
	public void close() throws IOException {
		if (degradedBlock != null) {
			degradedBlock.close();
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

	@Override
	public void seek(long arg0) throws IOException {
		
	}

	@Override
	public boolean seekToNewSource(long arg0) throws IOException {
		return false;
	}


}

