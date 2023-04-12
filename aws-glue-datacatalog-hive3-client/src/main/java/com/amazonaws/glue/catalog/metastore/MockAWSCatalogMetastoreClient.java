package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Mock metastore client. It uses MockStore
 */
public class MockAWSCatalogMetastoreClient implements IMetaStoreClient {

    private final HiveConf conf;
    private MockStore glueStore;

    public MockAWSCatalogMetastoreClient(HiveConf conf) throws MetaException {
        this(conf, null);
    }

    public MockAWSCatalogMetastoreClient(HiveConf conf, HiveMetaHookLoader hook) throws MetaException {
        this.conf = conf;
        String metaStore = conf.get("awsglue.catalog.store.content", "");
        initStore(metaStore);
    }

    private void initStore(String metaStore) {
        glueStore = MockStore.fromJson(metaStore);
    }

    @Override
    public Database getDatabase(String name) throws NoSuchObjectException, MetaException, TException {
        Database hiveDatabase = new Database();
        hiveDatabase.setName(name);
        hiveDatabase.setDescription("table description");
        String location = "/db";
        hiveDatabase.setLocationUri(location);
        hiveDatabase.setParameters(Maps.<String, String>newHashMap());
        return hiveDatabase;
    }

    @Override
    public Database getDatabase(String catalogName, String databaseName) throws NoSuchObjectException, MetaException, TException {
        return getDatabase(databaseName);
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException, TException {
        Set<String> dbnames = new HashSet<>();
        for (MockStore.MockTable table: glueStore.getTables()) {
            dbnames.add(table.getDbname());
        }
        return Lists.newArrayList(dbnames);
    }

    @Override
    public List<String> getDatabases(String catalogName, String databaseName) throws MetaException, TException {
        return getDatabases(databaseName);
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        return getDatabases(".*");
    }

    @Override
    public List<String> getAllDatabases(String s) throws MetaException, TException {
        return getAllDatabases();
    }

    @Override
    public List<String> getAllTables(String dbname) throws MetaException, TException, UnknownDBException {
        List<String> tablenames = new ArrayList<>();
        for (MockStore.MockTable table: glueStore.getTables()) {
            if (table.getDbname().equalsIgnoreCase(dbname)) {
                tablenames.add(table.getTablename());
            }
        }
        return tablenames;
    }

    @Override
    public List<String> getAllTables(String catName, String dbName) throws MetaException, TException, UnknownDBException {
        return getAllTables(dbName);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        MockStore.MockTable selectedTable = null;
        for (MockStore.MockTable table: glueStore.getTables()) {
            if (table.getDbname().equalsIgnoreCase(dbName) &&
                    table.getTablename().equalsIgnoreCase(tableName)) {
                selectedTable = table;
                break;
            }
        }

        if (selectedTable == null) {
            return null;
        }

        return getMockedHiveTable(dbName, tableName, selectedTable);
    }

    @Override
    public Table getTable(String catName, String dbName, String tableName) throws MetaException, TException {
        return getTable(dbName, tableName);
    }

    private Table getMockedHiveTable(String dbName, String tableName, MockStore.MockTable selectedTable) {
        Table hiveTable = new Table();
        hiveTable.setDbName(dbName);
        hiveTable.setTableName(tableName);
        hiveTable.setOwner("owner");
        Date createTime = new Date();
        hiveTable.setCreateTime((int) (createTime.getTime() / 1000));
        hiveTable.setLastAccessTime((int) (createTime.getTime() / 1000));
        hiveTable.setRetention(0);
        hiveTable.setSd(getStorageDescriptor(selectedTable));
        hiveTable.setPartitionKeys(Collections.<FieldSchema>emptyList());
        hiveTable.setParameters(getTableParameters(selectedTable));
        hiveTable.setViewOriginalText("");
        hiveTable.setViewExpandedText("");
        hiveTable.setTableType(selectedTable.getTabletype());
        return hiveTable;
    }

    private StorageDescriptor getStorageDescriptor(MockStore.MockTable selectedTable) {
        StorageDescriptor hiveSd = new StorageDescriptor();
        hiveSd.setCols(getFieldSchemas(selectedTable));
        hiveSd.setLocation(selectedTable.getLocation());
        hiveSd.setInputFormat(selectedTable.getInputformat());
        hiveSd.setOutputFormat(selectedTable.getOutputformat());
        hiveSd.setCompressed(false);
        hiveSd.setNumBuckets(-1);

        hiveSd.setSerdeInfo(getSerDeInfo(selectedTable));
        hiveSd.setBucketCols(Lists.<String>newArrayList());
        hiveSd.setSortCols(Collections.<Order>emptyList());
        hiveSd.setParameters(getTableParameters(selectedTable));
        hiveSd.setSkewedInfo(null);
        hiveSd.setStoredAsSubDirectories(false);
        return hiveSd;
    }

    private Map<String, String> getTableParameters(MockStore.MockTable selectedTable) {
        Map<String, String> parameters = Maps.<String, String>newHashMap();
        parameters.put("sizeKey", selectedTable.getSizekey());
        parameters.put("recordCount", selectedTable.getRecordcount());
        return parameters;
    }

    private SerDeInfo getSerDeInfo(MockStore.MockTable selectedTable) {
        SerDeInfo hiveSerDeInfo = new SerDeInfo();
        hiveSerDeInfo.setName(null);
        hiveSerDeInfo.setParameters(selectedTable.getSerdeprops());
        hiveSerDeInfo.setSerializationLib(selectedTable.getSerializationlib());
        return hiveSerDeInfo;
    }

    private List<FieldSchema> getFieldSchemas(MockStore.MockTable selectedTable) {
        List<FieldSchema> hiveFieldSchemaList = new ArrayList<>();
        for(MockStore.MockFieldSchema fieldSchema: selectedTable.getFields()) {
            FieldSchema hiveFieldSchema1 = new FieldSchema();
            hiveFieldSchema1.setType(fieldSchema.getType());
            hiveFieldSchema1.setName(fieldSchema.getName());
            hiveFieldSchema1.setComment("");
            hiveFieldSchemaList.add(hiveFieldSchema1);
        }
        return hiveFieldSchemaList;
    }

    /*****************************************************************
     * FOLLOWING METHODS ARE NOT IMPLEMENTED. THEY ARE PLACEHOLDERS *
    *****************************************************************/

    @Override
    public void createDatabase(Database database) throws InvalidObjectException,
            org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    }

    @Override
    public void alterDatabase(String databaseName, Database database) throws NoSuchObjectException, MetaException,
            TException {
    }

    @Override
    public void alterDatabase(String s, String s1, Database database) throws NoSuchObjectException, MetaException, TException {
    }

    @Override
    public void dropDatabase(String name) throws NoSuchObjectException, InvalidOperationException, MetaException,
            TException {
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) throws NoSuchObjectException,
            InvalidOperationException, MetaException, TException {
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    }

    @Override
    public void dropDatabase(String s, String s1, boolean b, boolean b1, boolean b2) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition add_partition(org.apache.hadoop.hive.metastore.api.Partition partition)
            throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
            TException {
        return partition;
    }

    @Override
    public int add_partitions(List<org.apache.hadoop.hive.metastore.api.Partition> partitions)
            throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
            TException {
        return partitions.size();
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> add_partitions(
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
            boolean ifNotExists,
            boolean needResult
    ) throws TException {
        return Collections.emptyList();
    }

    @Override
    public int add_partitions_pspec(
            PartitionSpecProxy pSpec
    ) throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
            MetaException, TException {
        return 0;
    }

    @Override
    public void alterFunction(String dbName, String functionName, org.apache.hadoop.hive.metastore.api.Function newFunction) throws InvalidObjectException,
            MetaException, TException {
    }

    @Override
    public void alterFunction(String s, String s1, String s2, Function function) throws InvalidObjectException, MetaException, TException {

    }

    public void alter_partition(
            String dbName,
            String tblName,
            org.apache.hadoop.hive.metastore.api.Partition partition
    ) throws InvalidOperationException, MetaException, TException {
    }

    @Override
    public void alter_partition(
            String dbName,
            String tblName,
            org.apache.hadoop.hive.metastore.api.Partition partition,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
    }

    @Override
    public void alter_partition(String s, String s1, String s2, Partition partition, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    public void alter_partitions(
            String dbName,
            String tblName,
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions
    ) throws InvalidOperationException, MetaException, TException {
    }

    @Override
    public void alter_partitions(
            String dbName,
            String tblName,
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
    }

    @Override
    public void alter_partitions(String s, String s1, String s2, List<Partition> list, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table table)
            throws InvalidOperationException, MetaException, TException {
    }

    @Override
    public void alter_table(String s, String s1, String s2, Table table, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

    }

    public void alter_table(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table table, boolean cascade)
            throws InvalidOperationException, MetaException, TException {
    }

    @Override
    public void alter_table_with_environmentContext(
            String dbName,
            String tblName,
            org.apache.hadoop.hive.metastore.api.Table table,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String dbName, String tblName, List<String> values)
            throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public Partition appendPartition(String s, String s1, String s2, List<String> list) throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String dbName, String tblName, String partitionName) throws InvalidObjectException,
            org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public Partition appendPartition(String s, String s1, String s2, String s3) throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public boolean create_role(org.apache.hadoop.hive.metastore.api.Role role) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean drop_role(String roleName) throws MetaException, TException {
        return false;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Role> list_roles(
            String principalName, org.apache.hadoop.hive.metastore.api.PrincipalType principalType
    ) throws MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse get_principals_in_role(
            org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest request) throws MetaException, TException {
        return null;
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest request) throws MetaException, TException {
        return null;
    }

    @Override
    public boolean grant_role(
            String roleName,
            String userName,
            org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
            String grantor, org.apache.hadoop.hive.metastore.api.PrincipalType grantorType,
            boolean grantOption
    ) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean revoke_role(
            String roleName,
            String userName,
            org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
            boolean grantOption
    ) throws MetaException, TException {
        return false;
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
    }

    @Override
    public String getTokenStrForm() throws IOException {
        return "";
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        return false;
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
        return false;
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
        return "";
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        return Collections.emptyList();
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
        return 0;
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {
    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
        return false;
    }

    @Override
    public String[] getMasterKeys() throws TException {
        return null;
    }

    @Override
    public LockResponse checkLock(long lockId)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public void commitTxn(long txnId) throws NoSuchTxnException, TxnAbortedException, TException {
    }

    @Override
    public void replCommitTxn(long l, String s) throws NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public void abortTxns(List<Long> txnIds) throws TException {
    }

    @Override
    public long allocateTableWriteId(long l, String s, String s1) throws TException {
        return 0;
    }

    @Override
    public void replTableWriteIdState(String s, String s1, String s2, List<String> list) throws TException {

    }

    @Override
    public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> list, String s, String s1) throws TException {
        return null;
    }

    @Override
    public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String s, String s1, String s2, List<TxnToWriteId> list) throws TException {
        return null;
    }

    @Deprecated
    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType
    ) throws TException {
    }

    @Deprecated
    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
    }

    @Override
    public CompactionResponse compact2(String s, String s1, String s2, CompactionType compactionType, Map<String, String> map) throws TException {
        return null;
    }

    @Override
    public void createFunction(org.apache.hadoop.hive.metastore.api.Function function) throws InvalidObjectException, MetaException, TException {
    }

    @Override
    public void createTable(org.apache.hadoop.hive.metastore.api.Table tbl) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, InvalidObjectException, MetaException,
            NoSuchObjectException, TException {
    }

    @Override
    public boolean deletePartitionColumnStatistics(
            String dbName, String tableName, String partName, String colName
    ) throws NoSuchObjectException, MetaException, InvalidObjectException,
            TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return false;
    }

    @Override
    public boolean deletePartitionColumnStatistics(String s, String s1, String s2, String s3, String s4) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean deleteTableColumnStatistics(
            String dbName, String tableName, String colName
    ) throws NoSuchObjectException, MetaException, InvalidObjectException,
            TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return false;
    }

    @Override
    public boolean deleteTableColumnStatistics(String s, String s1, String s2, String s3) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public void dropFunction(String dbName, String functionName) throws MetaException, NoSuchObjectException,
            InvalidObjectException, org.apache.hadoop.hive.metastore.api.InvalidInputException, TException {
    }

    @Override
    public void dropFunction(String s, String s1, String s2) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {

    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean dropPartition(String s, String s1, String s2, List<String> list, boolean b) throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, PartitionDropOptions options) throws TException {
        return false;
    }

    @Override
    public boolean dropPartition(String s, String s1, String s2, List<String> list, PartitionDropOptions partitionDropOptions) throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, String partitionName, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        List<String> values = partitionNameToVals(partitionName);
        return false;
    }

    @Override
    public boolean dropPartition(String s, String s1, String s2, String s3, boolean b) throws NoSuchObjectException, MetaException, TException {
        return false;
    }


//    @Override
//    public List<Partition> dropPartitions(String catName, String dbName, String tblName, List<org.apache.hadoop.hive.metastore.utils.ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists, boolean needResults) throws TException {
//        return IMetaStoreClient.super.dropPartitions(catName, dbName, tblName, partExprs, deleteData, ifExists, needResults);
//    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(
            String dbName,
            String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            boolean deleteData,
            boolean ifExists
    ) throws NoSuchObjectException, MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(
            String dbName,
            String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            boolean deleteData,
            boolean ifExists,
            boolean needResults
    ) throws NoSuchObjectException, MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(
            String dbName,
            String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            PartitionDropOptions options
    ) throws TException {
        return Collections.emptyList();
    }

    @Override
    public List<Partition> dropPartitions(String s, String s1, String s2, List<ObjectPair<Integer, byte[]>> list, PartitionDropOptions partitionDropOptions) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Deprecated
    public void dropTable(String tableName, boolean deleteData) throws MetaException, UnknownTableException, TException,
            NoSuchObjectException {
    }

    @Override
    public void dropTable(String dbname, String tableName) throws MetaException, TException, NoSuchObjectException {
    }

    @Override
    public void dropTable(String s, String s1, String s2, boolean b, boolean b1, boolean b2) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void truncateTable(String s, String s1, List<String> list) throws MetaException, TException {

    }

    @Override
    public void truncateTable(String s, String s1, String s2, List<String> list) throws MetaException, TException {

    }

    @Override
    public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest cmRecycleRequest) throws MetaException, TException {
        return null;
    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab)
            throws MetaException, TException, NoSuchObjectException {
    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge)
            throws MetaException, TException, NoSuchObjectException {
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(
            Map<String, String> partitionSpecs,
            String srcDb,
            String srcTbl,
            String dstDb,
            String dstTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public Partition exchange_partition(Map<String, String> map, String s, String s1, String s2, String s3, String s4, String s5) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(
            Map<String, String> partitionSpecs,
            String sourceDb,
            String sourceTbl,
            String destDb,
            String destTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> map, String s, String s1, String s2, String s3, String s4, String s5) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public AggrStats getAggrColStatsFor(String s, String s1, String s2, List<String> list, List<String> list1) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public String getConfigValue(String name, String defaultValue) throws TException, ConfigValSecurityException {
        if(!Pattern.matches("(hive|hdfs|mapred).*", name)) {
            throw new ConfigValSecurityException("For security reasons, the config key " + name + " cannot be accessed");
        }

        return conf.get(name, defaultValue);
    }

    @Override
    public String getDelegationToken(
            String owner, String renewerKerberosPrincipalName
    ) throws MetaException, TException {
        return "";
    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException,
            UnknownTableException, UnknownDBException {
        return Collections.emptyList();
    }

    @Override
    public List<FieldSchema> getFields(String s, String s1, String s2) throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Function getFunction(String dbName, String functionName) throws MetaException, TException {
        return null;
    }

    @Override
    public Function getFunction(String s, String s1, String s2) throws MetaException, TException {
        return null;
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<String> getFunctions(String s, String s1, String s2) throws MetaException, TException {
        return null;
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        return new GetAllFunctionsResponse();
    }

    @Override
    public String getMetaConf(String key) throws MetaException, TException {
        ConfVars metaConfVar = HiveConf.getMetaConf(key);
        if (metaConfVar == null) {
            throw new MetaException("Invalid configuration key " + key);
        }
        return conf.get(key, metaConfVar.getDefaultValue());
    }

    @Override
    public void createCatalog(Catalog catalog) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterCatalog(String s, Catalog catalog) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public Catalog getCatalog(String s) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<String> getCatalogs() throws MetaException, TException {
        return null;
    }

    @Override
    public void dropCatalog(String s) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, List<String> values)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public Partition getPartition(String s, String s1, String s2, List<String> list) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, String partitionName)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public Partition getPartition(String s, String s1, String s2, String s3) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName,
            String tableName,
            List<String> partitionNames,
            List<String> columnNames
    ) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String s, String s1, String s2, List<String> list, List<String> list1) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(
            String databaseName, String tableName, List<String> values,
            String userName, List<String> groupNames)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public Partition getPartitionWithAuthInfo(String s, String s1, String s2, List<String> list, String s3, List<String> list1) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(
            String databaseName, String tableName, List<String> partitionNames)
            throws NoSuchObjectException, MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<Partition> getPartitionsByNames(String s, String s1, String s2, List<String> list) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName) throws MetaException, TException, UnknownTableException,
            UnknownDBException {
        return Collections.emptyList();
    }

    @Override
    public List<FieldSchema> getSchema(String s, String s1, String s2) throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Deprecated
    public org.apache.hadoop.hive.metastore.api.Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
        //this has been deprecated
        return getTable("default", tableName);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String s, String s1, String s2, List<String> list) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(String dbName, List<String> tableNames) throws MetaException,
            InvalidOperationException, UnknownDBException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<Table> getTableObjectsByName(String s, String s1, List<String> list) throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return null;
    }

    @Override
    public Materialization getMaterializationInvalidationInfo(CreationMetadata creationMetadata, String s) throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return null;
    }

    @Override
    public void updateCreationMetadata(String s, String s1, CreationMetadata creationMetadata) throws MetaException, TException {

    }

    @Override
    public void updateCreationMetadata(String s, String s1, String s2, CreationMetadata creationMetadata) throws MetaException, TException {

    }

    @Override
    public List<String> getTables(String dbname, String tablePattern) throws MetaException, TException, UnknownDBException {
        return Collections.emptyList();
    }

    @Override
    public List<String> getTables(String s, String s1, String s2) throws MetaException, TException, UnknownDBException {
        return null;
    }

    public List<String> getTables(String dbname, String tablePattern, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        return Collections.emptyList();
    }

    @Override
    public List<String> getTables(String s, String s1, String s2, TableType tableType) throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String s) throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String s, String s1) throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
            throws MetaException, TException, UnknownDBException {
        return Collections.emptyList();
    }

    @Override
    public List<TableMeta> getTableMeta(String s, String s1, String s2, List<String> list) throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        return null;
    }

    @Override
    public ValidTxnList getValidTxns(long currentTxn) throws TException {
        return null;
    }

    @Override
    public ValidWriteIdList getValidWriteIds(String s) throws TException {
        return null;
    }

    @Override
    public List<TableValidWriteIds> getValidWriteIds(List<String> list, String s) throws TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet get_privilege_set(
            HiveObjectRef obj,
            String user, List<String> groups
    ) throws MetaException, TException {
        return null;
    }

    @Override
    public boolean grant_privileges(org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges)
            throws MetaException, TException {
        return false;
    }

    @Override
    public boolean revoke_privileges(
            org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges,
            boolean grantOption
    ) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean refresh_privileges(HiveObjectRef hiveObjectRef, String s, PrivilegeBag privilegeBag) throws MetaException, TException {
        return false;
    }

    @Override
    public void heartbeat(long txnId, long lockId)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        return null;
    }

    @Override
    public boolean isCompatibleWith(Configuration conf) {
        return true;
    }

    @Override
    public void setHiveAddedJars(String addedJars) {
    }

    @Override
    public boolean isLocalMetaStore() {
        return false;
    }

    @Override
    public boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partKVs, PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        return false;
    }

    @Override
    public boolean isPartitionMarkedForEvent(String s, String s1, String s2, Map<String, String> map, PartitionEventType partitionEventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        return false;
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, short max)
            throws MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<String> listPartitionNames(String s, String s1, String s2, int i) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName,
                                           List<String> values, short max)
            throws MetaException, TException, NoSuchObjectException {
        return Collections.emptyList();
    }

    @Override
    public List<String> listPartitionNames(String s, String s1, String s2, List<String> list, int i) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest partitionValuesRequest) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public int getNumPartitionsByFilter(String dbName, String tableName, String filter)
            throws MetaException, NoSuchObjectException, TException {
        return 0;
    }

    @Override
    public int getNumPartitionsByFilter(String s, String s1, String s2, String s3) throws MetaException, NoSuchObjectException, TException {
        return 0;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String dbName, String tblName, int max) throws TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String s, String s1, String s2, int i) throws TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String dbName, String tblName, String filter, int max)
            throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String s, String s1, String s2, String s3, int i) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String dbName, String tblName, short max)
            throws NoSuchObjectException, MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<Partition> listPartitions(String s, String s1, String s2, int i) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws NoSuchObjectException, MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<Partition> listPartitions(String s, String s1, String s2, List<String> list, int i) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public boolean listPartitionsByExpr(
            String databaseName,
            String tableName,
            byte[] expr,
            String defaultPartitionName,
            short max,
            List<org.apache.hadoop.hive.metastore.api.Partition> result
    ) throws TException {
        return false;
    }

    @Override
    public boolean listPartitionsByExpr(String s, String s1, String s2, byte[] bytes, String s3, int i, List<Partition> list) throws TException {
        return false;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(
            String databaseName,
            String tableName,
            String filter,
            short max
    ) throws MetaException, NoSuchObjectException, TException {
        return Collections.emptyList();
    }

    @Override
    public List<Partition> listPartitionsByFilter(String s, String s1, String s2, String s3, int i) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String database, String table, short maxParts,
                                                                                           String user, List<String> groups)
            throws MetaException, TException, NoSuchObjectException {
        return Collections.emptyList();
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String s, String s1, String s2, int i, String s3, List<String> list) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String database, String table,
                                                                                           List<String> partVals, short maxParts,
                                                                                           String user, List<String> groups) throws MetaException, TException, NoSuchObjectException {
        return Collections.emptyList();
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String s, String s1, String s2, List<String> list, int i, String s3, List<String> list1) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws MetaException,
            TException, InvalidOperationException, UnknownDBException {
        return Collections.emptyList();
    }

    @Override
    public List<String> listTableNamesByFilter(String s, String s1, String s2, int i) throws TException, InvalidOperationException, UnknownDBException {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(
            String principal,
            org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
            HiveObjectRef objectRef
    ) throws MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
        return null;
    }

    @Override
    public void markPartitionForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType
    ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
    }

    @Override
    public void markPartitionForEvent(String s, String s1, String s2, Map<String, String> map, PartitionEventType partitionEventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {

    }

    @Override
    public long openTxn(String user) throws TException {
        return 0;
    }

    @Override
    public List<Long> replOpenTxn(String s, List<Long> list, String s1) throws TException {
        return null;
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        return null;
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
        return null;
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
        return Collections.emptyList();
    }

    @Override
    public void reconnect() throws MetaException {
    }

    @Override
    public void renamePartition(String dbName, String tblName, List<String> partitionValues,
                                org.apache.hadoop.hive.metastore.api.Partition newPartition)
            throws InvalidOperationException, MetaException, TException {
    }

    @Override
    public void renamePartition(String s, String s1, String s2, List<String> list, Partition partition) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        return 0;
    }

    @Override
    public void rollbackTxn(long txnId) throws NoSuchTxnException, TException {
    }

    @Override
    public void replRollbackTxn(long l, String s) throws NoSuchTxnException, TException {

    }

    @Override
    public void setMetaConf(String key, String value) throws MetaException, TException {
        ConfVars confVar = HiveConf.getMetaConf(key);
        if (confVar == null) {
            throw new MetaException("Invalid configuration key " + key);
        }
        String validate = confVar.validate(value);
        if (validate != null) {
            throw new MetaException("Invalid configuration value " + value + " for key " + key + " by " + validate);
        }
        conf.set(key, value);
    }

    @Override
    public boolean setPartitionColumnStatistics(org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest request)
            throws NoSuchObjectException, InvalidObjectException,
            MetaException, TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return true;
    }

    @Override
    public void flushCache() {
        //no op
    }

    @Override
    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
        return null;
    }

    @Override
    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
            List<Long> fileIds,
            ByteBuffer sarg,
            boolean doGetFooters
    ) throws TException {
        return null;
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws TException {
    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
    }

    @Override
    public boolean isSameConfObj(Configuration conf) {
        //taken from HiveMetaStoreClient
        return this.conf == conf;
    }

    @Override
    public boolean cacheFileMetadata(String dbName, String tblName, String partName, boolean allParts) throws TException {
        return false;
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest uniqueConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest notNullConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest defaultConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest checkConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public void createTableWithConstraints(
            org.apache.hadoop.hive.metastore.api.Table table,
            List<SQLPrimaryKey> primaryKeys,
            List<SQLForeignKey> foreignKeys,
            List<SQLUniqueConstraint> uniqueConstraints,
            List<SQLNotNullConstraint> notNullConstraints,
            List<SQLDefaultConstraint> defaultConstraints,
            List<SQLCheckConstraint> checkConstraints
    ) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    }

    @Override
    public void dropConstraint(
            String dbName,
            String tblName,
            String constraintName
    ) throws MetaException, NoSuchObjectException, TException {
    }

    @Override
    public void dropConstraint(String s, String s1, String s2, String s3) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
            throws MetaException, NoSuchObjectException, TException {
    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
            throws MetaException, NoSuchObjectException, TException {
    }

    @Override
    public void addUniqueConstraint(List<SQLUniqueConstraint> list) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addNotNullConstraint(List<SQLNotNullConstraint> list) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addDefaultConstraint(List<SQLDefaultConstraint> list) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addCheckConstraint(List<SQLCheckConstraint> list) throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public String getMetastoreDbUuid() throws MetaException, TException {
        return null;
    }

    @Override
    public void createResourcePlan(WMResourcePlan wmResourcePlan, String s) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public WMFullResourcePlan getResourcePlan(String s) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<WMResourcePlan> getAllResourcePlans() throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void dropResourcePlan(String s) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public WMFullResourcePlan alterResourcePlan(String s, WMNullableResourcePlan wmNullableResourcePlan, boolean b, boolean b1, boolean b2) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return null;
    }

    @Override
    public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
        return null;
    }

    @Override
    public WMValidateResourcePlanResponse validateResourcePlan(String s) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void createWMTrigger(WMTrigger wmTrigger) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterWMTrigger(WMTrigger wmTrigger) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void dropWMTrigger(String s, String s1) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public List<WMTrigger> getTriggersForResourcePlan(String s) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void createWMPool(WMPool wmPool) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterWMPool(WMNullablePool wmNullablePool, String s) throws NoSuchObjectException, InvalidObjectException, TException {

    }

    @Override
    public void dropWMPool(String s, String s1) throws TException {

    }

    @Override
    public void createOrUpdateWMMapping(WMMapping wmMapping, boolean b) throws TException {

    }

    @Override
    public void dropWMMapping(WMMapping wmMapping) throws TException {

    }

    @Override
    public void createOrDropTriggerToPoolMapping(String s, String s1, String s2, boolean b) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void createISchema(ISchema iSchema) throws TException {

    }

    @Override
    public void alterISchema(String s, String s1, String s2, ISchema iSchema) throws TException {

    }

    @Override
    public ISchema getISchema(String s, String s1, String s2) throws TException {
        return null;
    }

    @Override
    public void dropISchema(String s, String s1, String s2) throws TException {

    }

    @Override
    public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {

    }

    @Override
    public SchemaVersion getSchemaVersion(String s, String s1, String s2, int i) throws TException {
        return null;
    }

    @Override
    public SchemaVersion getSchemaLatestVersion(String s, String s1, String s2) throws TException {
        return null;
    }

    @Override
    public List<SchemaVersion> getSchemaAllVersions(String s, String s1, String s2) throws TException {
        return null;
    }

    @Override
    public void dropSchemaVersion(String s, String s1, String s2, int i) throws TException {

    }

    @Override
    public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst findSchemasByColsRqst) throws TException {
        return null;
    }

    @Override
    public void mapSchemaVersionToSerde(String s, String s1, String s2, int i, String s3) throws TException {

    }

    @Override
    public void setSchemaVersionState(String s, String s1, String s2, int i, SchemaVersionState schemaVersionState) throws TException {

    }

    @Override
    public void addSerDe(SerDeInfo serDeInfo) throws TException {

    }

    @Override
    public SerDeInfo getSerDe(String s) throws TException {
        return null;
    }

    @Override
    public LockResponse lockMaterializationRebuild(String s, String s1, long l) throws TException {
        return null;
    }

    @Override
    public boolean heartbeatLockMaterializationRebuild(String s, String s1, long l) throws TException {
        return false;
    }

    @Override
    public void addRuntimeStat(RuntimeStat runtimeStat) throws TException {

    }

    @Override
    public List<RuntimeStat> getRuntimeStats(int i, int i1) throws TException {
        return null;
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        return null;
    }

    @Override
    public void addDynamicPartitions(
            long txnId,
            long writeId,
            String dbName,
            String tblName,
            List<String> partNames
    ) throws TException {
    }

    @Override
    public void addDynamicPartitions(
            long txnId,
            long writeId,
            String dbName,
            String tblName,
            List<String> partNames,
            DataOperationType operationType
    ) throws TException {
    }

    public void insertTable(org.apache.hadoop.hive.metastore.api.Table table, boolean overwrite) throws MetaException {
    }

    @Override
    public NotificationEventResponse getNextNotification(
            long lastEventId, int maxEvents, NotificationFilter notificationFilter) throws TException {
        return null;
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        return null;
    }

    @Override
    public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest notificationEventsCountRequest) throws TException {
        return null;
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
        return null;
    }

    @Override
    public ShowLocksResponse showLocks() throws TException {
        return null;
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        return null;
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        return null;
    }

    @Deprecated
    public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException {
        //this method has been deprecated;
        return false;
    }

    @Override
    public boolean tableExists(String databaseName, String tableName) throws MetaException, TException,
            UnknownDBException {
        return false;
    }

    @Override
    public boolean tableExists(String s, String s1, String s2) throws MetaException, TException, UnknownDBException {
        return false;
    }

    @Override
    public void unlock(long lockId) throws NoSuchLockException, TxnOpenException, TException {
    }

    @Override
    public boolean updatePartitionColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return true;
    }

    @Override
    public boolean updateTableColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return true;
    }

    @Override
    public void validatePartitionNameCharacters(List<String> part_vals) throws TException, MetaException {
    }

}
