package com.qcloud.emr;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.PreconditionMessage;
import alluxio.hadoop.HadoopUtils;
import alluxio.hadoop.FileSystem;
import alluxio.wire.MountPointInfo;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link FileSystem} interface. Any program
 * working with Hadoop HDFS can work with Alluxio transparently.
 */
public class CosNShimFileSystem extends FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(CosNShimFileSystem.class);

  Map<String, String> mMountTable;
  /**
   * Constructs a new {@link CosNShimFileSystem}.
   */
  public CosNShimFileSystem() {
    super();
  }

  /**
   * Constructs a new {@link CosNShimFileSystem} instance with a
   * specified {@link alluxio.client.file.FileSystem} handler for tests.
   *
   * @param fileSystem handler to file system
   */
  public CosNShimFileSystem(alluxio.client.file.FileSystem fileSystem) {
    super(fileSystem);
  }

  @Override
  public synchronized void initialize(URI uri, org.apache.hadoop.conf.Configuration conf,
      @Nullable AlluxioConfiguration alluxioConfiguration)
      throws IOException {
    super.initialize(uri, conf, alluxioConfiguration);
    try {
      Map<String, MountPointInfo> mountTable = mFileSystem.getMountTable();
      mMountTable = new HashMap<>();
      for (Map.Entry<String, MountPointInfo> entry: mountTable.entrySet()) {
        String mMountPoint = entry.getKey();
        String ufsUri = entry.getValue().getUfsUri();
        if (!ufsUri.startsWith(CosNShimAbstractFileSystem.SHIM_COSN_SCHEMA)) {
          continue;
        }
        if (!mMountPoint.endsWith("/")) {
          mMountPoint += "/";
        }
        if (!ufsUri.endsWith("/")) {
          ufsUri += "/";
        }
        LOG.info("Add mMountPoint:{} ufsUri:{}", mMountPoint, ufsUri);
        mMountTable.put(mMountPoint, ufsUri);
      }
    } catch (AlluxioException e) {
      throw new IOException("Failed to load namespaces ", e);
    }
  }

  @Override
  public String getScheme() {
    return CosNShimAbstractFileSystem.SHIM_COSN_SCHEMA;
  }

  @Override
  protected void validateFsUri(URI fsUri) throws IllegalArgumentException {
    Preconditions.checkArgument(fsUri.getScheme().equals(getScheme()),
        PreconditionMessage.URI_SCHEME_MISMATCH.toString(), fsUri.getScheme(), getScheme());
  }

  @Override
  protected Path getFsPath(String fsUriHeader, URIStatus fileStatus) throws IOException {
    String cosPath = HadoopUtils.getCosNPathByAlluxioPath(mMountTable,
        fileStatus.getPath());
    if (cosPath == null) {
      throw new IOException(String.format("Failed to convert Alluxio path %s to CosN,"
              + " check if it had mounted", fileStatus.getPath()));
    }
    return new Path(cosPath);
  }

  @Override
  protected AlluxioURI getAlluxioPath(Path path) throws IOException {
    String alluxioPath = HadoopUtils.getAlluxioPathByCosNPath(mMountTable, path.toString());
    if (alluxioPath == null) {
      throw new IOException(String.format("Failed to convert CosN path %s to Alluxio, "
          + "check if it had mounted ", path));
    }
    return new AlluxioURI(alluxioPath);
  }
}
