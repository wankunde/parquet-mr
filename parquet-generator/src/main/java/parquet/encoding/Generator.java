/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.encoding;

import parquet.encoding.bitpacking.ByteBasedBitPackingGenerator;
import parquet.encoding.bitpacking.IntBasedBitPackingGenerator;

/**
 * main class for code generation hook in build for encodings generation
 *
 * <pre>
 *   这个真的是无语了，代码生成居然是运行main方法，手动传入参数来生成？
 *
 *   能不能优雅点哈～～
 *
 * main运行参数: /Users/wankun/ws/wankun/parquet-mr/parquet-encoding/src/main/java
 * </pre>
 *
 * @author Julien Le Dem
 *
 */
public class Generator {

  public static void main(String[] args) throws Exception {
    IntBasedBitPackingGenerator.main(args);
    ByteBasedBitPackingGenerator.main(args);
  }

}
