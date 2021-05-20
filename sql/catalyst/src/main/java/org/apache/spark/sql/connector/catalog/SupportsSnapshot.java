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
 *
 */

package org.apache.spark.sql.connector.catalog;

import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;

public interface SupportsSnapshot extends TableCatalog {

  /**
   * Creates a table based off of an already existing table which can be modified without modifying
   * the original table.
   *
   * @param sourceCatalog Catalog containing the original table
   * @param sourceIdent the identifier for the original table
   * @param ident the new snapshot table identifier
   * @param properties a string map of table properties
   * @return metadata for the new table
   * @throws TableAlreadyExistsException If a table or view already exists for the identifier
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  Table snapshotTable(
      TableCatalog sourceCatalog,
      Identifier sourceIdent,
      Identifier ident,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException;

}
