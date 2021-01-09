package org.apache.spark.sql.connector.write;

import org.apache.spark.sql.connector.read.ScanBuilder;

/**
 * An interface for building a scan and a write for a row-level operation.
 */
public interface MergeBuilder {
  /**
   * Creates a scan builder for a row-level operation.
   *
   * @return a scan builder
   */
  ScanBuilder asScanBuilder();

  /**
   * Creates a write builder for a row-level operation.
   *
   * @return a write builder
   */
  WriteBuilder asWriteBuilder();
}
