package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock client factory that return mock metastore client
 */
public class MockAWSGlueDataCatalogHiveClientFactory implements HiveMetaStoreClientFactory {

  @Override
  public IMetaStoreClient createMetaStoreClient(
          HiveConf conf, HiveMetaHookLoader hookLoader,
          boolean allowEmbedded,
          ConcurrentHashMap<String, Long> concurrentHashMap
  ) throws MetaException {
    MockAWSCatalogMetastoreClient client = new MockAWSCatalogMetastoreClient(conf, hookLoader);
    return client;
  }

}
