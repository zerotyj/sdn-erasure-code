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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author padicao
 */
public class FileStripeInfo implements Writable  {
    private String filePath;
    private List<BlockStripeInfo> blockStripes;
    
    public FileStripeInfo(String f) {
        filePath = f;
        this.blockStripes = new ArrayList<BlockStripeInfo>();
    }
    
    public FileStripeInfo() {
        this.blockStripes = new ArrayList<BlockStripeInfo>();
    }
    
    public void addBlockStripes(int index, BlockStripeInfo b) {
        for(int i = blockStripes.size(); i <= index; i++) {
            blockStripes.add(null);
        }
        blockStripes.set(index, b);
    }

    public String getFilePath() {
        return filePath;
    }

    public List<BlockStripeInfo> getBlockStripes() {
        return blockStripes;
    }
    
    public BlockStripeInfo getBlockStripe(int index) {
        if(index < 0 || index >= blockStripes.size()) {
            return null;
        }
        return blockStripes.get(index);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{ ");
        sb.append(filePath).append(" : { ");
        for(BlockStripeInfo b : blockStripes) {
            sb.append(b.toString()).append(" ");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(filePath);
        out.writeInt(blockStripes.size());
        for(BlockStripeInfo b : blockStripes) {
            b.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        filePath = in.readUTF();
        int len = in.readInt();
        for(int i = 0; i < len; i++) {
            BlockStripeInfo b = new BlockStripeInfo();
            b.readFields(in);
            blockStripes.add(b);
        }
    }
}
