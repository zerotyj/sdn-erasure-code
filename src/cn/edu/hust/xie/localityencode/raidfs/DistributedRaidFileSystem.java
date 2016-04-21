/*
 * Copyright 2015 Apache Software Foundation.
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @author padicao
 */
public class DistributedRaidFileSystem extends FileSystem {

    public static final URI RAIDFS_FS_URI = URI.create("raidfs:///");
    public static final URI HDFS_FS_URI = URI.create("hdfs:///");
    private static final Path RAIDFS_PATH = new Path(RAIDFS_FS_URI);
    private static final Path HDFS_PATH = new Path(HDFS_FS_URI);

    private Path workingDir;
    private FsServerDefaults serverDefaults;

    public static final Log LOG = LogFactory.getLog(DistributedRaidFileSystem.class);
    private DistributedFileSystem hdfs;


    public DistributedRaidFileSystem() {
        //LOG.info("Start DistributedRaidFileSystem");
        
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        hdfs = (DistributedFileSystem) FileSystem.get(conf);
        FsServerDefaults hdfsDefaults = hdfs.getServerDefaults();
        serverDefaults = new FsServerDefaults(hdfsDefaults.getBlockSize(),
                hdfsDefaults.getBytesPerChecksum(),
                hdfsDefaults.getWritePacketSize(), (short) 1,
                hdfsDefaults.getFileBufferSize(), hdfsDefaults.getEncryptDataTransfer(),
                hdfsDefaults.getTrashInterval(), hdfsDefaults.getChecksumType());
    }
    
    @Override
    public void close() throws IOException {
        hdfs.close();
        super.close();
    }

    public FileSystem getHDFSFileSystem() {
        return hdfs;
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return serverDefaults.getBlockSize();
    }

    @Override
    public short getDefaultReplication(Path path) {
        return 1;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file,
            long start, long len) throws IOException {
        Path p = toHdfsPath(file.getPath());
        return hdfs.getFileBlockLocations(p, start, len);
    }

    @Override
    public FsServerDefaults getServerDefaults() {
        return serverDefaults;
    }

    @Override
    public String getScheme() {
        return RAIDFS_FS_URI.getScheme();
    }

    public Path toHdfsPath(Path f) {
        Path p = Path.getPathWithoutSchemeAndAuthority(f);
        p = hdfs.makeQualified(p);
        return p;
    }

    public Path toRaidfsPath(Path f) {
        Path p = Path.getPathWithoutSchemeAndAuthority(f);
        p = this.makeAbsolute(p);
        p = this.makeQualified(p);
        return p;
    }

    private Path makeAbsolute(Path f) {
        if (f.isAbsolute()) {
            return f;
        } else {
            return new Path(workingDir, f);
        }
    }

    @Override
    public Path getHomeDirectory() {
        return this.makeQualified(new Path("/user", System
                .getProperty("user.name")));
    }

    @Override
    public URI getUri() {
        return RAIDFS_FS_URI;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        Path hdfsPath = toHdfsPath(path);
        return new FSDataInputStream(new RaidfsInputStream(hdfs, hdfsPath, bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission,
            boolean override, int bufferSize, short replication, long blockSize,
            Progressable progress) throws IOException {
        Path hdfsPath = toHdfsPath(path);
        return hdfs.create(hdfsPath, permission, override, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize,
            Progressable progress) throws IOException {
        Path hdfsPath = toHdfsPath(path);
        return hdfs.append(hdfsPath, bufferSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dest) throws IOException {
        Path srcHdfsPath = toHdfsPath(src);
        Path destHdfsPath = toHdfsPath(dest);
        return hdfs.rename(srcHdfsPath, destHdfsPath);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        Path hdfsPath = toHdfsPath(path);
        return hdfs.delete(hdfsPath, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        //LOG.info("enter listStatus");
        Path hdfsPath = toHdfsPath(path);
        FileStatus[] listStatus = hdfs.listStatus(hdfsPath);
        for (FileStatus s : listStatus) {
            s.setPath(toRaidfsPath(s.getPath()));
            LOG.info(s.toString());
        }
        //LOG.info("leave listStatus");
        return listStatus;
    }

    @Override
    public void setWorkingDirectory(Path path) {
        //LOG.info("enter setWorkingDirectory");
        checkPath(path);
        workingDir = makeAbsolute(path);
        checkPath(workingDir);
        //LOG.info("leaving setWorkingDirectory");
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        Path hdfsPath = toHdfsPath(path);
        return hdfs.mkdirs(hdfsPath, permission);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        //LOG.info("enter getFileStatus");
        Path hdfsPath = toHdfsPath(path);
        FileStatus fileStatus = hdfs.getFileStatus(hdfsPath);
        fileStatus.setPath(toRaidfsPath(fileStatus.getPath()));
        //LOG.info("leaving getFileStatus");
        return fileStatus;
    }
}
