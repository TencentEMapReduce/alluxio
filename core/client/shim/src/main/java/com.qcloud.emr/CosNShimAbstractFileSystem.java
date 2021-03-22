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

package com.qcloud.emr;

import alluxio.conf.PropertyKey;
import alluxio.hadoop.FileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link FileSystem} interface. Any program
 * working with Hadoop HDFS can work with Alluxio transparently. Note that the performance of using
 * this API may not be as efficient as the performance of using the Alluxio native API defined in
 * {@link org.apache.hadoop.fs.DelegateToFileSystem}.
 */
class CosNShimAbstractFileSystem extends DelegateToFileSystem {
  static final String SHIM_COSN_SCHEMA = "cosn";

  /**
   * This constructor has the signature needed by //todo,explain this.
   *
   * @param uri  the uri for this cosn filesystem
   * @param conf Hadoop configuration
   * @throws URISyntaxException if <code>uri</code> has syntax error
   */
  public CosNShimAbstractFileSystem(final URI uri, final Configuration conf)
      throws IOException, URISyntaxException {
    super(uri, new FileSystem(), conf, SHIM_COSN_SCHEMA, false);
  }

  @Override
  public int getUriDefaultPort() {
    return Integer.parseInt(PropertyKey.MASTER_RPC_PORT.getDefaultValue());
  }
}
