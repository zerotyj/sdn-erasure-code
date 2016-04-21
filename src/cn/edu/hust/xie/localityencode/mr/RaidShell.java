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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 *
 * @author padicao
 */
public class RaidShell {

    private Configuration conf;
    private RaidNodeProtocol client;

    private void initialize() throws IOException {
        conf = new Configuration();
        client = RaidNode.getRaidNodeClient(conf);
    }

    private void distRaid(String path, int type) throws IOException {
        boolean status = client.raidFile(path, type);
        System.out.println("RaidNode return " + status);
    }

    private void raidStripe(String path, int index) throws IOException {
        if(index < 0) {
            FileStripeInfo fileStripe = client.getFileStripeInfo(path);
            System.out.println(fileStripe.toString());
        } else {
            BlockStripeInfo blockStripe = client.getBlockStripeInfo(path, index);
            System.out.println(blockStripe.toString());
        }
    }
    
    private void check(String path) throws IOException {
        int count = client.checkDataset(path);
        System.out.println(String.format("Need to move %d blocks", count));
    }
    
    private void clearCache() throws IOException {
        client.clearCache();
    }

    private void close() {
        if (client != null) {
            RPC.stopProxy(client);
            client = null;
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: RaidShell distRaid/raidStripe/check ...");
            return;
        }
        RaidShell shell = new RaidShell();

        try {
            shell.initialize();
            if (args[0].equals("distRaid")) {
                if (args.length != 3) {
                    System.err.println("Usage: RaidShell distRaid path type");
                    return;
                }
                shell.distRaid(args[1], Integer.valueOf(args[2]));
            } else if (args[0].equals("raidStripe")) {
                if (args.length != 3) {
                    System.err.println("Usage: RaidShell raidStripe path blockIndex");
                    return;
                }
                shell.raidStripe(args[1], Integer.valueOf(args[2]));
            } else if(args[0].equals("check")) {
                if(args.length != 2) {
                    System.err.println("Usage: RaidShell check path");
                    return;
                }
                shell.check(args[1]);
            } else if(args[0].equals("clear")) {
                shell.clearCache();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            shell.close();
        }
    }
}
