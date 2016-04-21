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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author padicao
 */
public class LocalityECMapReduce extends RandomECMapReduce {

    private static final Log LOG = LogFactory.getLog(LocalityECMapReduce.class);
    
    // Locality 算法跟 Random 的区别便在，Locality 选择拥有块数目最多的节点运行编码任务
    @Override
    protected void setupPreferHost(TaskInfo t, 
            Map<String, Integer> rackMap,
            Map<String, Integer> nodeMap) {
        String selected = "";
        int maxCount = 0;
        //for(Map.Entry<String, Integer> entry : nodeMap.entrySet()) {
        for(Map.Entry<String, Integer> entry : rackMap.entrySet()) {
            if(maxCount < entry.getValue()) {
                selected = entry.getKey();
                maxCount = entry.getValue();
            }
        }
        
        t.setHost(node.getRandomHostInRack(selected));
        
        /*
        List<String> hosts = node.getHostsInRack(selected);
        maxCount = 0;
        for(String h : hosts) {
            Integer count = nodeMap.get(h);
            if(count != null && count > maxCount) {
                maxCount = count;
                selected = h;
            }
        }
        t.setHost(selected);
        */
    }

    // 遍历目录/文件，选择条带
    @Override
    public void selectTask() throws IOException {
        super.selectTask();
        Collections.sort(this.tasks);
    }
}
