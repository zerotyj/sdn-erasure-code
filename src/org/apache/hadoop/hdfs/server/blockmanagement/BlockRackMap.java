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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author padicao
 */
public class BlockRackMap implements Writable {
    private Map<Long, String> preferRacks;

    public BlockRackMap(Map<Long, String> racks) {
        this.preferRacks = racks;
    }

    public BlockRackMap() {
        this.preferRacks = new HashMap<Long, String>();
    }

    public Map<Long, String> getPreferRacks() {
        return preferRacks;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(preferRacks.size());
        for(Map.Entry<Long, String> entry : preferRacks.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for(int i = 0; i < size; i++) {
            long blockId = in.readLong();
            String rack = in.readUTF();
            preferRacks.put(blockId, rack);
        }
    }
}
