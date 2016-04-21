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
import cn.edu.hust.xie.localityencode.main.ec.XorCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * @author padicao
 */
public class RaidConfig {

    public static String STRIPE_SIZE = "mapreduce.localityencode.stripesize";
    public static String PARITY_SIZE = "mapreduce.localityencode.paritysize";
    public static String CODE_CLASS = "mapreduce.localityencode.code.class";
    public static String RAID_PORT = "mapreduce.localityencode.raidnode.address";
    public static String BLOCK_PLACEMENT_ADDRESS = "mapreduce.localityencode.blockplacement.address";
    public static String DIST_3RR = "mapreduce.localityencode.dist.3rr";
    public static String SET_REPLICAS = "mapreduce.localityencode.set.replicas";

    public static ErasureCode getErasureCode(Configuration conf) {
        Class<? extends ErasureCode> codeClass = conf.getClass(CODE_CLASS, XorCode.class, ErasureCode.class);
        ErasureCode ec = ReflectionUtils.newInstance(codeClass, null);
        
        int stripe = conf.getInt(STRIPE_SIZE, 3);
        int parity = conf.getInt(PARITY_SIZE, 2);
        ec.initialize(stripe, parity);
        return ec;
    }

    public static String getRaidNodeAddress(Configuration conf) {
        return conf.get(RAID_PORT, "localhost:37760");
    }

    // for example : do raid for /user/root/dir
    // ParityPath : /raidfs/user/root/dir
    // MetaData : /raidfs/user/root/dir.meta
    // MR Output Path : /raidfs/user/root/dir/mr
    // Parity File : /raidfs/user/root/dir/mr/raid-1-0.1000 /raidfs/user/root/dir/mr/raid-1-1.1000 ...
    // Parity Merged File Temp : /raidfs/user/root/dir/mr/raid-1.1000
    // Parity Merged File : /raidfs/user/root/dir/mr/raid-1 /raidfs/user/root/dir/mr/raid-0
    // Parity Final Merged File Temp : /raidfs/user/root/mr/raid-final.1000
    // Parity Final Merged File : /raidfs/user/roor/dir/raid-final
    public static Path getParityPath(Path dataPath) {
        return Path.mergePaths(new Path("/raidfs"), dataPath);
    }

    public static String getMetaDataName() {
        return ".meta";
    }

    public static String getMRPathName() {
        return "mr";
    }

    public static String getParityFileFinalName() {
        return "raid-final";
    }

    public static String getParityFileFinalTempName(long ts) {
        return String.format("raid-final.%d", ts);
    }

    public static String getParityFileName(int stripeIndex) {
        return String.format("raid-%d", stripeIndex);
    }

    public static String getParityFileTempName(int stripeIndex, long ts) {
        return String.format("raid-%d.%d", stripeIndex, ts);
    }

    public static String getParityPartFileTempName(int stripeIndex, int parityIndex, long ts) {
        return String.format("raid-%d-%d.%d", stripeIndex, parityIndex, ts);
    }
}
