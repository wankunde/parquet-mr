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
package parquet.filter;


/**
 * Filter to be applied to a record to work out whether to skip it.
 *
 * <pre>
 *  function: 判断数据是否满足内部的predicate
 *
 *  实现子类
 *  * ColumnRecordFilter: 用户根据自己需求编写自己 ColumnPredicates.Predicate filterPredicate 的实现类，对
 *  指定数据进行过滤规则，内部的ColumnReader会先读取对应的数据，然后应用filterPredicate，判断数据是否满足要求。
 *
 *  * PagedRecordFilter: 只读取指定范围(startPos, startPos + pageSize)内的记录
 *
 * </pre>
 * @author Jacob Metcalf
 */
public interface RecordFilter {

  /**
   * Works out whether the current record can pass through the filter.
   */
  boolean isMatch();

}
