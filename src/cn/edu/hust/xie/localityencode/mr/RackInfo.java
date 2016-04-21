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
public class RackInfo implements Comparable<RackInfo> {

    RackInfo(String r) {
        this.rack = r;
        this.blocks = new ArrayList<BlockInfo>();
    }
    String rack;
    List<BlockInfo> blocks;

    @Override
    public int compareTo(RackInfo o) {
        return Integer.compare(blocks.size(), o.blocks.size());
    }
    
}
