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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class for using URI with .
 */
@ThreadSafe
public final class URIUtils {
  private static final Logger LOG = LoggerFactory.getLogger(URIUtils.class);

  public static final String PREFIX_PATH = "/";
  public static final String NOT_MATCH = "";

  public static final ConcurrentHashMap<String, String> EQUAL_SCHEMA = initEqualSchema();

  private static ConcurrentHashMap<String, String> initEqualSchema() {
    ConcurrentHashMap<String, String> m = new ConcurrentHashMap<String, String>();
    m.put(CosNShimTransparentFileSystem.COSN_SCHEME,
        CosNShimTransparentFileSystem.COSN_SHIM_SCHEME);

    return m;
  }

  /**
   * TODO(SUNDY),.
   * @param  thisSchema an schema
   * @param  thatSchema an schema
   * @return the final prefix path{@link Path}
   * */
  public static boolean matchSchema(final String thisSchema, final String thatSchema) {
    if (thisSchema.equals(thatSchema)) {
      return true;
    }
    String t = EQUAL_SCHEMA.get(thisSchema);
    if (t != null && t.equals(thatSchema)) {
      return true;
    }
    t = EQUAL_SCHEMA.get(thatSchema);
    if (t != null && t.equals(thisSchema)) {
      return true;
    }
    return false;
  }

  /**
   * TODO(SUNDY),.
   * @param  thisAuthority an schema
   * @param  thatAuthority an schema
   * @param  conf an hadoop conf
   * @return the final prefix path{@link Path}
   * */
  public static boolean matchAuthority(final String thisAuthority, final String thatAuthority,
      Configuration conf) {
    LOG.debug("matchAuthority-> thisAuthority:{}, thatAuthority:{}", thisAuthority, thatAuthority);
    if (thisAuthority.equals(thatAuthority)) {
      return true;
    }
    //we think cosn://bucket/ -> cosn://bucket-appId/ is equals
    //so, we will deal with appId
    //it will remove the appId,
    return removeAppIdAuthority(thisAuthority, conf)
        .equals(removeAppIdAuthority(thatAuthority, conf));
  }

  private static String removeAppIdAuthority(final String oriAuthority, Configuration conf) {
    String appId = conf.get(CosNConfigKeys.COSN_APPID_KEY);
    LOG.debug("removeAppIdAuthority-> oriAuthority:{}, appId:{}",
        oriAuthority, appId);
    String[] authorities = oriAuthority.split("-");
    if (authorities.length < 1) {
      return oriAuthority;
    }
    if (!authorities[authorities.length - 1].equals(appId)) {
      return oriAuthority;
    }
    StringBuilder sb = new StringBuilder();
    sb.append(authorities[0]);
    for (int i = 1; i < authorities.length - 1; i++) {
      sb.append("-").append(authorities[i]);
    }
    return sb.toString();
  }

  /**
   * TODO(SUNDY),.
   * @param  thisUri an java URI {@link URI}
   * @param  thatUri an java URI {@link URI}
   * @param  conf an hadoop conf
   * @return the final prefix path{@link Path}
   * */
  public static String matchUri(final URI thisUri, final URI thatUri, Configuration conf) {
    LOG.debug("matchUri-> thisUri:{}, thatUri:{}", thisUri, thatUri);
    //1.schema+authority match
    if (!matchSchema(thisUri.getScheme(), thatUri.getScheme())) {
      LOG.debug("matchUri-> NOT_MATCH  matchSchema.");
      return NOT_MATCH;
    }
    if (!matchAuthority(thisUri.getAuthority(), thatUri.getAuthority(), conf)) {
      LOG.debug("matchUri-> NOT_MATCH  matchAuthority.");
      return NOT_MATCH;
    }

    //2.strPath match, there can/need not use Dijkstra to match
    //eg. /a/b vs /a/b1 not match
    //    /a/b    /a/b/c match match
    //so, it will split using separator path, then orderly traverse sub-path
    //TODO(SUNDY),there only support linux separator.
    String[] thisPaths = thisUri.getPath().split("/");
    if (thisPaths.length == 1) { //this is root, all will match
      return PREFIX_PATH;
    }
    String[] thatPaths = thatUri.getPath().split("/");
    if (thisPaths.length > thatPaths.length) {
      return NOT_MATCH;
    }
    for (int i = 0; i < thisPaths.length; i++) {
      if (!thisPaths[i].equals(thatPaths[i])) {
        return  NOT_MATCH;
      }
    }

    //combine path
    String[] finalPaths = Arrays.copyOfRange(thatPaths, thisPaths.length, thatPaths.length);
    StringBuilder sb = new StringBuilder();
    sb.append(PREFIX_PATH);
    for (String path:finalPaths) {
      sb.append(path).append("/");
    }
    return  sb.toString();
  }

  private URIUtils() {} // prevent instantiation
}
