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
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.MountPointInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link FileSystem}
 * interface. Any program working with Hadoop HDFS can work with Alluxio transparently. Note that
 * the performance of using this API may not be as efficient as the performance of using the Alluxio
 * native API defined in {@link alluxio.client.file.FileSystem}, which this API is built on top of.
 */
public class TransparentFileSystem extends FileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(TransparentFileSystem.class);

  public static final String SCHEME = "transparent";
  public static final String HEADER = SCHEME + "://";
  public static final String DEFAULT_URI = "alluxio:///";
  public static final String ALLUXIO_TRANSPARENT_ENABLE = "alluxio.client.transparent.enabled";
  public static final String ALLUXIO_TRANSPARENT_BY_PASS_PATHS =
      "alluxio.client.transparent.by-pass.paths";
  private FileSystem mShimFs;
  private alluxio.hadoop.FileSystem mAlluxioFs;
  private Configuration mConf;
  private InstancedConfiguration mAlluxioConf;
  private String mOriSchema;
  private alluxio.client.file.FileSystem mAlluxioFsClient;
  private Map<String, MountPointInfo> mMountTable;
  private Path mWorkingDir;

  /**
   * Constructs a new {@link TransparentFileSystem} instance.
   */
  public TransparentFileSystem() {}

  /**
   * This constructor has the signature needed by //todo,explain this.
   *
   * @param uri  the uri for this Alluxio filesystem
   * @param conf Hadoop configuration
   * @throws URISyntaxException if <code>uri</code> has syntax error
   */
  public TransparentFileSystem(final URI uri, final Configuration conf)
      throws IOException, URISyntaxException {
    initialize(uri, conf);
  }

  /**
   * it will according to uri, generate diffirent filesystem. it need
   * fs.AbstractFileSystem.hdfs.impl. to alluxio.hadoop.TransparentFileSystem in core-site.xml.
   */
  @Override
  public void initialize(final URI uri, final Configuration conf)
      throws IOException {
    super.initialize(uri, conf);
    mConf = conf;

    mAlluxioConf = new InstancedConfiguration(ConfigurationUtils.defaults());
    FileSystemContext mAlluxioContext = FileSystemContext.create(mAlluxioConf);
    mAlluxioFsClient = alluxio.client.file.FileSystem.Factory.create(mAlluxioContext);

    mWorkingDir = new Path("/");

    initAlluxioFs(uri);
    getOrGenFileSystem(uri);
  }

  private FileSystem getOrGenFileSystem(final URI uri)
      throws IOException {
    //1.determine whether uri had been mapping
    //we will according mountedPointed to generate dealing FileSystem
    LOG.debug("it will getOrGenFileSystem, use uri:{}, path:{}, host:{}, fragment:{} conf:{}",
        uri, uri.getPath(), uri.getHost(), uri.getFragment(), mConf);
    if (hadMountedPoint(uri)) {
      mShimFs = mAlluxioFs;
    } else { //only not match
      URI myUri = genNoChangeSchemeURI(uri);
      mShimFs = FileSystem.get(myUri, mConf);
      if (mShimFs == null) {
        throw new NullPointerException("mShimFs is null");
      }
      LOG.debug("it had getOrGenFileSystem, mShimFs use uri:{}, path:{}, authority:{}, "
              + "fragment:{} conf:{}", mShimFs.getUri(), mShimFs.getUri().getPath(),
          mShimFs.getUri().getAuthority(),
          mShimFs.getUri().getFragment(), mShimFs.getConf());
    }
    return  mShimFs;
  }

  private void initAlluxioFs(final URI uri) {
    LOG.debug("initAlluxioFs , uri:{}", uri);
    try {
      mAlluxioFs = new alluxio.hadoop.FileSystem();
      //here we use DEFAULT_URI, because considering simply deal with.
      //need use ln alluxio.site.conf.dir to /etc/alluxio,detail AbstractFileSystem.initialize
      //may be it also use uri + authority from alluxioConf ?
      // alluxio uri only use schema + authority
      String p = getMountedTblPath(uri);
      URI initUri = new URI(Constants.SCHEME, null, p, null);
      LOG.debug("initAlluxioFs , initUri:{}", initUri);
      mAlluxioFs.initialize(initUri, mConf);
    } catch (URISyntaxException | IOException e) {
      e.printStackTrace();
    }
    LOG.debug("initAlluxioFs success, mAlluxioFs:{}, mAlluxioFs.getUri {}", mAlluxioFs,
        mAlluxioFs.getUri());
  }

  private URI genNoChangeSchemeURI(final URI uri) throws IOException {
    URI thatUri = null;
    switch (uri.getScheme()) {
      case CosNShimTransparentFileSystem.COSN_SCHEME:
        LOG.debug("it will generate CosnFilesystem，oriUri:{}", uri);
        //here we must had configured fs.shimcosn.impl
        // to alluxio.hadoop.shim.CosShimTransparentFileSystem
        try {
          thatUri = new URI(CosNShimTransparentFileSystem.COSN_SHIM_SCHEME, uri.getAuthority(),
              uri.getPath(), null, null);
        } catch (URISyntaxException e) {
          e.printStackTrace();
          LOG.error("genNoChangeScheme err:{}", e.getMessage());
        }
        break;
      case CosNShimTransparentFileSystem.COSN_SHIM_SCHEME:
        return uri;
      default:
        throw new IOException(uri.getScheme() + " is Not Supported TransparentFileSystem");
    }
    LOG.debug("it had genNoChangeSchemeURI, thatUri:{}", thatUri);
    return thatUri;
  }

  private boolean hadMountedPoint(final URI uri) throws IOException {
    if (uri.getScheme().equals(Constants.SCHEME)) {
      return true;
    }
    //1.enable transparent.
    LOG.debug("ALLUXIO_TRANSPARENT_ENABLE:{}",
        mConf.getBoolean(ALLUXIO_TRANSPARENT_ENABLE, false));
    if (!mConf.getBoolean(ALLUXIO_TRANSPARENT_ENABLE, false)) {
      return false;
    }
    //2.not in by pass paths,TODO(sundy)
    String byPassPaths = mConf.get(ALLUXIO_TRANSPARENT_BY_PASS_PATHS);
    LOG.debug("ALLUXIO_TRANSPARENT_BY_PASS_PATHS:{}", byPassPaths);

    //3.in mounted points
    if (mMountTable == null) {
      getMountInfo();
    }
    for (Map.Entry<String, MountPointInfo> entry : mMountTable.entrySet()) {
      String mMountPoint = entry.getKey();
      MountPointInfo mountPointInfo = entry.getValue();
      LOG.debug(":-> {} Mounted on {}", mMountPoint, mountPointInfo.getUfsUri());
      LOG.debug(":-> check Uri: {}", uri);
      //TODO(sundy), there may is Performance issue- we can bloom filter path by seperate,
      //or other method
      //TODO(sundy),这里可能会有性能问题--反复多次NEW URI,可以挂载到mMountTable中去
      try {
        URI ufsUri = new URI(mountPointInfo.getUfsUri());
        return !URIUtils.matchUri(ufsUri, uri, mConf).equals(URIUtils.NOT_MATCH);
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }
    }
    return false;
  }

  private void getMountInfo() throws IOException {
    //TODO(sundy), it can checkStatus had modify, not need each connect to  rpc master.
    //otherwise, it will have performance problem.
    if (mMountTable != null) {
      return;
    }
    try {
      mMountTable = mAlluxioFsClient.getMountTable();
    } catch (AlluxioException e) {
      LOG.error("getMountInfo err:{}", e.getMessage());
      return;
    }
    for (Map.Entry<String, MountPointInfo> entry : mMountTable.entrySet()) {
      String mMountPoint = entry.getKey();
      MountPointInfo mountPointInfo = entry.getValue();
      LOG.debug("{} Mounted on {}", mMountPoint, mountPointInfo.getUfsUri());
    }
  }

  //通过uri来转换，
  //uri分成部分，Schema://Authority(IP+Port)/strPath,所以需要转换三个部分
  //1.转换schema+authority
  //2.转换strPath
  private URI getOrGenParentFsUri(final URI oriUri)
      throws IOException, URISyntaxException {
    LOG.debug("getOrGenParentUri ori URI: {}", oriUri);
    if (oriUri.getScheme().equals(Constants.SCHEME)) {
      LOG.debug("getOrGenParentUri Uri beyond alluxio.it need not to transfer");
      return oriUri;
    }
    if (!hadMountedPoint(oriUri)) {
      LOG.debug("getOrGenParentFsUri use genNoChangeSchemeURI URI: {}", oriUri);
      return genNoChangeSchemeURI(oriUri);
    }

    String p = getMountedTblPath(oriUri);
    if (p != null) {
      return new URI(Constants.SCHEME, mAlluxioFs.getUri().getAuthority(),
          p, null, null);
    }

    //FIXME(sundy), there should throw an Exception. hadMountedPoint() had bug
    throw new NullPointerException("getOrGenParentFsUri URI is null");
  }

  private String getMountedTblPath(URI oriUri) throws IOException {
    if (mMountTable == null) {
      getMountInfo();
    }
    for (Map.Entry<String, MountPointInfo> entry : mMountTable.entrySet()) {
      String mMountPoint = entry.getKey();
      MountPointInfo mountPointInfo = entry.getValue();
      try {
        LOG.debug("getMountedTblPath checking {} --mounted--> {}",
            mMountPoint, mountPointInfo.getUfsUri());
        URI ufsUri = new URI(mountPointInfo.getUfsUri());
        String suffixPath =  URIUtils.matchUri(ufsUri, oriUri, mConf);
        if (!suffixPath.equals(URIUtils.NOT_MATCH)) {
          //USE alluxioUri;
          String finalPath = mMountPoint.concat(suffixPath);
          LOG.debug("it matched alluxio :{} --mounted-->{}. suffixPath:{}, finPath:{}",
              mMountPoint, mountPointInfo.getUfsUri(), suffixPath, finalPath);
          return finalPath;
        }
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  //通过uri来转换，
  //uri分成部分，schema://Authority/strPath,所以需要转换三个部分
  private Path transferPath(final Path oriPath) throws IOException {
    LOG.debug("transferPath->it ori Path: {}", oriPath);
    Path thatPath = null;
    try {
      URI oriUri = getUriPath(oriPath);
      LOG.debug("transferPath->it getUriPath Uri: {}", oriUri);
      if (oriUri.getScheme().equals(Constants.SCHEME)) {
        LOG.debug("transferPath Uri beyond alluxio.it need not to transfer");
        return oriPath;
      }
      URI thatUri = getOrGenParentFsUri(oriUri);
      if (thatUri != null) {
        thatPath =  new Path(thatUri);
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    LOG.debug("transferPath->it that Path: {}", thatPath);
    return thatPath;
  }
  /**
   * Make the path Absolute and get the path-part of a pathname.
   * Checks that URI matches this file system
   * and that the path-part is a valid name.
   *
   * @param p path
   * @return path-part of the Path p
   */
  private URI getUriPath(final Path p) throws URISyntaxException {
    if (p.toUri() == null || p.toUri().getScheme() == null) {
      Path absolutePath = makeAbsolute(p);
      Path completePath = absolutePath.makeQualified(mShimFs.getUri(), mShimFs.getHomeDirectory());
      return new URI(completePath.toString());
    } else {
      return new URI(p.toString());
    }
  }

  private Path makeAbsolute(final Path f) {
    return f.isAbsolute() ? f : new Path(mWorkingDir, f);
  }

  @Override
  public URI getUri() {
    LOG.debug("Override mShimFs:{}, getUri:{}", mShimFs, mShimFs.getUri());
    return mShimFs.getUri();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    LOG.debug("it will generate transferPath，mShimFs:{},open Path:{}", mShimFs, f);
    Path thatPath = transferPath(f);
    LOG.debug("it use  thatPath getUri，open:{}", thatPath);
    return mShimFs.open(thatPath, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    LOG.debug("it will generate transferPath，mShimFs:{}, create:{}", mShimFs, f);
    Path thatPath = transferPath(f);
    LOG.debug("it use  thatPath getUri，create:{}", thatPath);
    return mShimFs.create(thatPath, permission, overwrite, bufferSize,
        replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    LOG.debug("it will generate transferPath，append:{}", f);
    Path thatPath = transferPath(f);
    LOG.debug("it use  thatPath getUri，append:{}", thatPath);
    return mShimFs.append(thatPath, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.debug("it will generate transferPath，rename:{}->{}", src, dst);
    Path thatSrcPath = transferPath(src);
    Path thatDstPath = transferPath(dst);
    LOG.debug("it rename:{}->{}", thatSrcPath, thatDstPath);
    return mShimFs.rename(thatSrcPath, thatDstPath);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.debug("it will generate transferPath，delete:{}", f);
    Path thatPath = transferPath(f);
    LOG.debug("it use  thatPath getUri，delete:{}", thatPath);
    return mShimFs.delete(thatPath, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    LOG.debug("it will generate transferPath，mShimFs:{}, listStatus Path:{}", mShimFs, f);
    Path thatPath = transferPath(f);
    LOG.debug("it use  thatPath getUri，listStatus:{}", thatPath);
    return mShimFs.listStatus(thatPath);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    LOG.debug("it will generate transferPath，mShimFs:{}, setWorkingDirectory:{}",
        mShimFs, new_dir);
    Path thatPath = null;
    try {
      thatPath = transferPath(new_dir);
    } catch (IOException e) {
      e.printStackTrace();
    }
    LOG.debug("it use  thatPath getUri，setWorkingDirectory:{}", thatPath);
    mShimFs.setWorkingDirectory(thatPath);
  }

  @Override
  public Path getWorkingDirectory() {
    LOG.debug("it will getScheme，mShimFs:{}, getWorkingDirectory:{}",
        mShimFs, mShimFs.getWorkingDirectory());
    return mShimFs.getWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    LOG.debug("it will generate transferPath，mShimFs:{}, mkdirs:{}", mShimFs, f);
    Path thatPath = transferPath(f);
    LOG.debug("it use  thatPath getUri，mkdirs:{}", thatPath);
    return mShimFs.mkdirs(thatPath, permission);
  }

  @Override
  public String getScheme() {
    //LOG.debug("Override，getScheme:{}", mShimFs.getScheme());
    return SCHEME;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    LOG.debug("it will generate getFileStatus，mShimFs:{}, append:{}", mShimFs, f);
    Path thatPath = transferPath(f);
    LOG.debug("it use  thatPath getUri，getFileStatus:{}", thatPath);
    return mShimFs.getFileStatus(thatPath);
  }

  /**
   * Check that a Path belongs to this FileSystem.
   * @param path to check
   */
  protected void checkPath(Path path) {
    LOG.debug("it will generate transferPath，checkPath:{}", path);
    try {
      Path thatPath = transferPath(path);
      LOG.debug("it use had checkedPath :{}", thatPath);
      super.checkPath(thatPath);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void access(Path path, FsAction mode) throws IOException {
    LOG.debug("it will generate transferPath，access:{}", path);
    Path thatPath = transferPath(path);
    LOG.debug("it use had checkedPath :{}", thatPath);
    mShimFs.access(thatPath, mode);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
      throws IOException {
    return mShimFs.getFileBlockLocations(file, start, len);
  }

  @Override
  public boolean setReplication(Path path, short replication) throws IOException {
    LOG.debug("it will generate transferPath，setReplication:{}", path);
    Path thatPath = transferPath(path);
    LOG.debug("it use had checkedPath :{}", thatPath);
    return mShimFs.setReplication(thatPath, replication);
  }

  /**
   * Changes owner or group of a path (i.e. a file or a directory). If username is null, the
   * original username remains unchanged. Same as groupname. If username and groupname are non-null,
   * both of them will be changed.
   *
   * @param path path to set owner or group
   * @param username username to be set
   * @param groupname groupname to be set
   */
  @Override
  public void setOwner(Path path, final String username, final String groupname)
      throws IOException {
    LOG.debug("it will generate transferPath，setOwner:{}", path);
    Path thatPath = transferPath(path);
    LOG.debug("it use had checkedPath :{}", thatPath);
    mShimFs.setOwner(thatPath, username, groupname);
  }

  /**
   * Changes permission of a path.
   *
   * @param path path to set permission
   * @param permission permission set to path
   */
  @Override
  public void setPermission(Path path, FsPermission permission) throws IOException {
    LOG.debug("it will generate transferPath，setPermission:{}", path);
    Path thatPath = transferPath(path);
    LOG.debug("it use had checkedPath :{}", thatPath);
    mShimFs.setPermission(thatPath, permission);
  }

  /**
   * Qualify a path to one which uses this FileSystem and, if relative,
   * made absolute.
   * @param path to qualify
   * @return this path if it contains a scheme and authority and is absolute, or
   * a new path that includes a path and authority and is fully qualified
   * @see Path#makeQualified(URI, Path)
   * @throws IllegalArgumentException if the path has a schema/URI different
   * from this FileSystem.
   */
  public Path makeQualified(Path path) {
    LOG.debug("it will generate transferPath，makeQualified:{}", path);
    try {
      Path thatPath = transferPath(path);
      LOG.debug("it use had checkedPath :{}", thatPath);
      return super.makeQualified(thatPath);
    } catch (IOException e) {
      e.printStackTrace();
    }
    //todo(sundy), there
    return null;
  }

  /**
   * The src file is on the local disk.  Add it to the filesystem at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO failure
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
      Path src, Path dst)
      throws IOException {
    LOG.debug("it will generate transferPath，copyFromLocalFile:{}->{}", src, dst);
    Path thatSrcPath = transferPath(src);
    Path thatDstPath = transferPath(dst);
    LOG.debug("it copyFromLocalFile:{}->{}", thatSrcPath, thatDstPath);
    mShimFs.copyFromLocalFile(delSrc, overwrite, thatSrcPath, thatDstPath);
  }

  /**
   * The src file is under this filesystem, and the dst is on the local disk.
   * Copy it from the remote filesystem to the local dst name.
   * delSrc indicates if the src will be removed
   * or not. useRawLocalFileSystem indicates whether to use RawLocalFileSystem
   * as the local file system or not. RawLocalFileSystem is non checksumming,
   * So, It will not create any crc files at local.
   *
   * @param delSrc
   *          whether to delete the src
   * @param src
   *          path
   * @param dst
   *          path
   * @param useRawLocalFileSystem
   *          whether to use RawLocalFileSystem as local file system or not.
   *
   * @throws IOException for any IO error
   */
  public void copyToLocalFile(boolean delSrc, Path src, Path dst,
      boolean useRawLocalFileSystem) throws IOException {
    LOG.debug("it will generate transferPath，copyToLocalFile:{}->{}", src, dst);
    Path thatSrcPath = transferPath(src);
    Path thatDstPath = transferPath(dst);
    LOG.debug("it copyToLocalFile:{}->{}", thatSrcPath, thatDstPath);
    mShimFs.copyToLocalFile(delSrc, thatSrcPath, thatDstPath, useRawLocalFileSystem);
  }

  /**
   * Truncate the file in the indicated path to the indicated size.
   * <ul>
   *   <li>Fails if path is a directory.</li>
   *   <li>Fails if path does not exist.</li>
   *   <li>Fails if path is not closed.</li>
   *   <li>Fails if new size is greater than current size.</li>
   * </ul>
   * @param f The path to the file to be truncated
   * @param newLength The size the file is to be truncated to
   *
   * @return <code>true</code> if the file has been truncated to the desired
   * <code>newLength</code> and is immediately available to be reused for
   * write operations such as <code>append</code>, or
   * <code>false</code> if a background process of adjusting the length of
   * the last block has been started, and clients should wait for it to
   * complete before proceeding with further file updates.
   * @throws IOException IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *         (default).
   */
  public boolean truncate(Path f, long newLength) throws IOException {
    LOG.debug("it will generate transferPath，truncate:{}", f);
    Path thatPath = transferPath(f);
    LOG.debug("it use had truncate :{}", thatPath);
    return mShimFs.truncate(thatPath, newLength);
  }

  /** Return the {@link ContentSummary} of a given {@link Path}.
   * @param f path to use
   * @throws FileNotFoundException if the path does not resolve
   * @throws IOException IO failure
   * @return ContentSummary
   */
  public ContentSummary getContentSummary(Path f) throws IOException {
    LOG.debug("it will generate transferPath，getContentSummary:{}", f);
    Path thatPath = transferPath(f);
    LOG.debug("it use had getContentSummary :{}", thatPath);
    return mShimFs.getContentSummary(thatPath);
  }

  /**
   * Mark a path to be deleted when its FileSystem is closed.
   * When the JVM shuts down cleanly, all cached FileSystem objects will be
   * closed automatically. These the marked paths will be deleted as a result.
   * The path must exist in the filesystem at the time of the method call;
   * it does not have to exist at the time of JVM shutdown.
   * @param f the path to delete
   * @return  true if deleteOnExit is successful, otherwise false
   * @throws IOException IO failure
   */
  public boolean deleteOnExit(Path f) throws IOException {
    LOG.debug("it will generate transferPath，deleteOnExit:{}", f);
    Path thatPath = transferPath(f);
    LOG.debug("it use had deleteOnExit :{}", thatPath);
    return mShimFs.deleteOnExit(thatPath);
  }

  /**
   * Cancel the scheduled deletion of the path when the FileSystem is closed.
   * @param f the path to cancel deletion
   * @return true if the path was found in the delete-on-exit list
   */
  public boolean cancelDeleteOnExit(Path f) {
    LOG.debug("it will generate transferPath，cancelDeleteOnExit:{}", f);
    Path thatPath = null;
    try {
      thatPath = transferPath(f);
    } catch (IOException e) {
      e.printStackTrace();
    }
    LOG.debug("it use had deleteOnExit :{}", thatPath);
    return mShimFs.cancelDeleteOnExit(thatPath);
  }

  @Override
  public Path resolvePath(final Path f)
      throws IOException {
    LOG.debug("it will generate transferPath，resolvePath:{}", f);
    Path thatPath = null;
    try {
      thatPath = transferPath(f);
    } catch (IOException e) {
      e.printStackTrace();
    }
    LOG.debug("it use had resolvePath :{}", thatPath);
    return mShimFs.resolvePath(thatPath);
  }
}
