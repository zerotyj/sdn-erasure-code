/*
 * Copyright 2015 Apache Software Foundation.
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
package cn.edu.hust.xie.localityencode.raidfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;

/**
 *
 * @author padicao
 */
public class RaidFs extends DelegateToFileSystem {

  RaidFs(final URI theUri, final Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, new DistributedRaidFileSystem(), conf, 
            DistributedRaidFileSystem.RAIDFS_FS_URI.getScheme(), true);
  }
  
  @Override
  public int getUriDefaultPort() {
    return -1;
  }
  
  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return fsImpl.getServerDefaults();
  }
}
