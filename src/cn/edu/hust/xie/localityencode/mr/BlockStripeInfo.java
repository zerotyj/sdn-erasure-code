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
public class BlockStripeInfo implements Writable {

    private List<BlockInfo> blocks;
    private int stripeIndex;

    public BlockStripeInfo() {
        blocks = new ArrayList<BlockInfo>();
    }

    public BlockStripeInfo(int index) {
        blocks = new ArrayList<BlockInfo>();
        stripeIndex = index;
    }
    
    public int getStripeIndex() {
        return stripeIndex;
    }

    public void addBlockInfo(BlockInfo b) {
        blocks.add(b);
    }

    public List<BlockInfo> getBlocks() {
        return blocks;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("stripeIndex").append(" { ");
        for (BlockInfo b : blocks) {
            sb.append(b.toString()).append(" ");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(stripeIndex);
        out.writeInt(blocks.size());
        for (BlockInfo b : blocks) {
            b.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stripeIndex = in.readInt();
        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            BlockInfo b = new BlockInfo();
            b.readFields(in);
            blocks.add(b);
        }
    }
}
