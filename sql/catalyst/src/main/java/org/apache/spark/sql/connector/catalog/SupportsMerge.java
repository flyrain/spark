package org.apache.spark.sql.connector.catalog;

import org.apache.spark.sql.connector.write.MergeBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;

/**
 * A mix-in interface for Table to indicate that it supports row-level operations.
 * <p>
 * This adds {@link #newMergeBuilder(String, LogicalWriteInfo)} that is used to create a scan and
 * a write for a row-level operation.
 */
public interface SupportsMerge extends Table {
  /**
   * Returns a {@link MergeBuilder} which can be used to create both a scan and a write for a row-level
   * operation. Spark will call this method to configure each data source row-level operation.
   *
   * @param operation operation
   * @param info write info
   * @return a merge builder
   */
  MergeBuilder newMergeBuilder(String operation, LogicalWriteInfo info);
}
