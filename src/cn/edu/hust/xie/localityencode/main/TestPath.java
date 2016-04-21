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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

/**
 *
 * @author padicao
 */
public class TestPath {
    public static void main(String[] args) throws UnsupportedFileSystemException {
        Configuration conf = new Configuration();
        FileContext fc = FileContext.getFileContext(conf);
        Path p = new Path("1.txt");
        System.out.println(String.format("Path : %s || %s", p.toString(), p.toUri()));
        p = fc.makeQualified(p);
        System.out.println(String.format("Path : %s || %s", p.toString(), p.toUri()));
        p = new Path(p, "");
        System.out.println(String.format("Path : %s || %s", p.toString(), p.toUri()));
        p = Path.mergePaths(new Path("/raid"), p);
        System.out.println(String.format("Path : %s || %s", p.toString(), p.toUri()));
    }
}
