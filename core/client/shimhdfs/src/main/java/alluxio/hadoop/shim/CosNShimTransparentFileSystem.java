/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop.shim;

import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link FileSystem}
 * interface. Any program working with Hadoop HDFS can work with Alluxio transparently. Note that
 * the performance of using this API may not be as efficient as the performance of using the Alluxio
 * native API defined in {@link alluxio.client.file.FileSystem}, which this API is built on top of.
 */
public class CosNShimTransparentFileSystem extends CosFileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(CosNShimTransparentFileSystem.class);
  static final String COSN_SHIM_SCHEME = "shimcosn";
  static final String COSN_SCHEME = "cosn";

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return <code>cosn</code>
   */
  @Override
  public String getScheme() {
    return CosNShimTransparentFileSystem.COSN_SHIM_SCHEME;
  }
}
