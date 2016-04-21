/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.hust.xie.localityencode.main.ec;

/**
 *
 * @author yvanhom
 */
public interface ErasureCode {
    // Initialize ErasureCode with s Raw data blocks and p coded blocks
    // for some codes, stripeCount() may not equals to s, parityCount() may not equals to p;
    public void initialize(int s, int p);
    
    // Get the number of raw data blocks in a stripe
    public int stripeCount();
    
    // Get the number of coded blocks in a stripe
    public int parityCount();
    
    // encode datas, write result to parities
    // every byte array should have at least length bytes
    // datas should have at least stripeCount() elements
    // parities should have at least paritityCount() elements
    public void encode(byte[][] datas, byte[][] parities, int length);
    
    // Find locations needed for recovering erased blocks
    // 
    // When the number of erased blocks is too large, throws TooManyErasedLocations
    public int[] locationsToReadForDecode(int[] erased) throws TooManyErasedLocations;
    
    // Do recovery decode
    // Elements in datas and parities can be nil.
    // But those in erased and locationsToReadForDecode() should not be nil.
    public void decode(byte[][] datas, byte[][] parities, int length, int[] erased);
}
