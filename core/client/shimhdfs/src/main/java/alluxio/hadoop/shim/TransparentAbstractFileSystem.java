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

import alluxio.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URI;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link org.apache.hadoop.fs.FileSystem}
 * interface. Any program working with Hadoop HDFS can work with Alluxio transparently. Note that
 * the performance of using this API may not be as efficient as the performance of using the Alluxio
 * native API defined in {@link alluxio.client.file.FileSystem}, which this API is built on top of.
 */
public class TransparentAbstractFileSystem extends DelegateToFileSystem {

  /**
   * This constructor has the signature needed by.
   *
   * @param uri  the uri for this Alluxio filesystem
   * @param conf Hadoop configuration
   * @throws URISyntaxException if <code>uri</code> has syntax error
   */
  TransparentAbstractFileSystem(final URI uri, final Configuration conf)
      throws IOException, URISyntaxException {
    //TODO(sundy),FileSystem will deal
    super(uri, new alluxio.hadoop.FileSystem(), conf, Constants.SCHEME, false);
  }
}