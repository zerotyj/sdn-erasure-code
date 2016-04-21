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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author padicao
 */
public class TaskInfo implements Comparable<TaskInfo> {

    private String host;
    private int stripeIndex;
    private List<BlockInfo> blocks;
    
    TaskInfo(String r, int stripeIndex) {
        this.host = r;
        this.blocks = new ArrayList<BlockInfo>();
        this.stripeIndex = stripeIndex;
    }

    void addBlock(BlockInfo b) {
        blocks.add(b);
    }

    public String getHost() {
        return host;
    }
    
    public void setHost(String h) {
        this.host = h;
    }

    public List<BlockInfo> getBlocks() {
        return blocks;
    }

    public int getStripeIndex() {
        return stripeIndex;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{ ");
        sb.append(host).append(" : ");
        for(BlockInfo b : blocks) {
            sb.append(b).append(" ");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public int compareTo(TaskInfo o) {
        return host.compareTo(o.getHost());
    }
}
