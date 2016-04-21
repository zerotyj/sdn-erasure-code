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
package cn.edu.hust.xie.localityencode.main;

import cn.edu.hust.xie.localityencode.mr.MapOnly;
import cn.edu.hust.xie.localityencode.mr.RaidNode;
import cn.edu.hust.xie.localityencode.mr.RaidShell;
import java.util.Arrays;


/**
 *
 * @author padicao
 */
public class Main {
    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
            System.err.println("Usage: Main map/raidshell/raidnode/writeonenode/testpath/testread/testjera");
            return;
        }
        String[] newargs = Arrays.copyOfRange(args, 1, args.length);
        if(args[0].equals("map")) {
            MapOnly.main(newargs);
        } else if(args[0].equals("writeonenode")) {
            TestWriteOneNode.main(newargs);
        } else if(args[0].equals("testpath")) {
            TestPath.main(newargs);
        } else if(args[0].equals("raidshell")) {
            RaidShell.main(newargs);
        } else if(args[0].equals("raidnode")) {
            RaidNode.main(newargs);
        } else if(args[0].equals("testread")) {
            TestRaidfsInputStream.main(newargs);
        } else if(args[0].equals("testjera")) {
            TestJeraRS.main(newargs);
        } else {
            System.err.println("Unrecognize command " + args[0]);
        }
    }
}
