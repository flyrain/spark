/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A mix-in interface for {@link Table} to indicate it supports the OPTIMIZE command.
 */
public interface SupportsOptimize extends Table {

  /**
   * Returns the type of rows produced by the OPTIMIZE command.
   */
  StructType optimizeOutput();

  /**
   * Bin-packs files in the table.
   *
   * @param filters filter expressions used to select files to be rewritten
   * @param options options to configure the execution
   * @return rows representing a summary of the execution
   */
  InternalRow[] binPack(Filter[] filters, CaseInsensitiveStringMap options);

  /**
   * Orders files in the table.
   *
   * @param filters filter expressions used to select files to be rewritten
   * @param requestedOrdering the requested ordering (empty if none)
   * @param options options to configure the execution
   * @return rows representing a summary of the execution
   */
  InternalRow[] orderBy(
      Filter[] filters,
      SortOrder[] requestedOrdering,
      CaseInsensitiveStringMap options);
}
