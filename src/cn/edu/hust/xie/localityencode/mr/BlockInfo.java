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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author padicao
 */
public class BlockInfo implements Writable {

    private String path;
    private int blockIndex;
    
    public static BlockInfo EMPTY_BLOCK_INFO = new BlockInfo("", -1);

    public BlockInfo(String p, int i) {
        this.path = p;
        this.blockIndex = i;
    }

    public BlockInfo() {
    }

    public String getPath() {
        return path;
    }

    public int getBlockIndex() {
        return blockIndex;
    }

    @Override
    public boolean equals(Object b) {
        if (!(b instanceof BlockInfo)) {
            return false;
        }
        BlockInfo bi = (BlockInfo) b;
        return blockIndex == bi.blockIndex
                && path.equals(bi.path);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + Objects.hashCode(this.path);
        hash = 59 * hash + this.blockIndex;
        return hash;
    }

    @Override
    public String toString() {
        return String.format("%s:%d", path, blockIndex);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(path);
        out.writeInt(blockIndex);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = in.readUTF();
        blockIndex = in.readInt();
    }
}
