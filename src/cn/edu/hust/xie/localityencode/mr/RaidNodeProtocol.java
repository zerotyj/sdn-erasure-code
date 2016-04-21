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
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 *
 * @author padicao
 */
public interface RaidNodeProtocol extends VersionedProtocol {

    public static final long versionID = 1L;  

    public FileStripeInfo getFileStripeInfo(String filePath) throws IOException;
    
    public BlockStripeInfo getBlockStripeInfo(String filePath, int blockIndex) throws IOException;
    
    public boolean raidFile(String filePath, int type) throws IOException;
    
    public String[] getCodeBlockPreferHost(String filePath, int stripeIndex, int count);
    
    public int checkDataset(String path) throws IOException;
    
    public void clearCache() throws IOException;
}
