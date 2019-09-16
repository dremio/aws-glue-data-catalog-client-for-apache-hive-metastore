package com.amazonaws.glue.shims;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.common.util.HiveVersionInfo;

public final class ShimsLoader {

  private static AwsGlueHiveShims hiveShims;

  public static synchronized AwsGlueHiveShims getHiveShims() {
    if (hiveShims == null) {
      hiveShims = loadHiveShims();
    }
    return hiveShims;
  }

  private static AwsGlueHiveShims loadHiveShims() {
    String hiveVersion = HiveVersionInfo.getShortVersion();
    if (AwsGlueSparkHiveShims.supportsVersion(hiveVersion)) {
      try {
        return AwsGlueSparkHiveShims.class.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("unable to get instance of Hive 1.x shim class");
      }
    } else {
      try {
        return AwsGlueHive2Shims.class.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("unable to get instance of Hive 2.x shim class");
      }
    }
  }

  @VisibleForTesting
  static synchronized void clearShimClass() {
    hiveShims = null;
  }

}
