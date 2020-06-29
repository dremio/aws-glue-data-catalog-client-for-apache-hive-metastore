package com.amazonaws.glue.catalog.convertors;

import com.amazonaws.glue.catalog.converters.HiveToCatalogConverter;
import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.SkewedInfo;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.UserDefinedFunction;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class EntityConversionTest {

  private static final String TEST_DB_NAME = "testDb";
  private static final String TEST_TBL_NAME = "testTbl";

  @Test
  public void testDatabaseConversion() {
    Database catalogDb = TestObjects.getTestDatabase();
    org.apache.hadoop.hive.metastore.api.Database hiveDatabase = CatalogToHiveConverter
        .convertDatabase(catalogDb);
    Database catalogDb2 = HiveToCatalogConverter.convertDatabase(hiveDatabase);
    assertEquals(catalogDb, catalogDb2);
  }

  @Test
  public void testDatabaseConversionWithNullFields() {
    Database catalogDb = TestObjects.getTestDatabase();
    catalogDb.setLocationUri(null);
    catalogDb.setParameters(null);
    org.apache.hadoop.hive.metastore.api.Database hiveDatabase = CatalogToHiveConverter
        .convertDatabase(catalogDb);
    assertThat(hiveDatabase.getLocationUri(), is(""));
    assertNotNull(hiveDatabase.getParameters());
  }

  @Test
  public void testExceptionTranslation() {
    assertEquals("org.apache.hadoop.hive.metastore.api.AlreadyExistsException",
        CatalogToHiveConverter.wrapInHiveException(new AlreadyExistsException("")).getClass().getName());
  }

  @Test
  public void testTableConversion() {
    Table catalogTable = TestObjects.getTestTable();
    org.apache.hadoop.hive.metastore.api.Table hiveTable = CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertEquals(catalogTable, HiveToCatalogConverter.convertTable(hiveTable));
  }
  
  @Test
  public void testTableConversionWithNullParameterMap() {
    // Test to ensure the parameter map returned to Hive is never null.
    Table catalogTable = TestObjects.getTestTable();
    catalogTable.setParameters(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getParameters());
    assertTrue(hiveTable.getParameters().isEmpty());
  }

  @Test
  public void testPartitionConversion() {
    Partition partition = TestObjects.getTestPartition(TEST_DB_NAME, TEST_TBL_NAME, ImmutableList.of("1"));
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = CatalogToHiveConverter.convertPartition(partition);
    Partition converted = HiveToCatalogConverter.convertPartition(hivePartition);
    assertEquals(partition, converted);
  }
  
  @Test
  public void testPartitionConversionWithNullParameterMap() {
    // Test to ensure the parameter map returned to Hive is never null.
    Partition partition = TestObjects.getTestPartition(TEST_DB_NAME, TEST_TBL_NAME, ImmutableList.of("1"));
    partition.setParameters(null);
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = CatalogToHiveConverter.convertPartition(partition);
    assertNotNull(hivePartition.getParameters());
    assertTrue(hivePartition.getParameters().isEmpty());
  }

  @Test
  public void testConvertPartitions() {
    Partition partition = TestObjects.getTestPartition(TEST_DB_NAME, TEST_TBL_NAME, ImmutableList.of("value1", "value2"));
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = CatalogToHiveConverter.convertPartition(partition);
    List<Partition> partitions = ImmutableList.of(partition);
    assertEquals(ImmutableList.of(hivePartition), CatalogToHiveConverter.convertPartitions(partitions));
  }

  @Test
  public void testConvertPartitionsEmpty() {
    assertEquals(ImmutableList.of(), CatalogToHiveConverter.convertPartitions(ImmutableList.<Partition>of()));
  }

  @Test
  public void testConvertPartitionsNull() {
    assertEquals(null, CatalogToHiveConverter.convertPartitions(null));
  }
  
  @Test
  public void testSkewedInfoConversion() {
    SkewedInfo catalogSkewedInfo = TestObjects.getSkewedInfo();         
    org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedinfo = CatalogToHiveConverter.convertSkewedInfo(catalogSkewedInfo);
    assertEquals(catalogSkewedInfo, HiveToCatalogConverter.convertSkewedInfo(hiveSkewedinfo));
    assertEquals(null, HiveToCatalogConverter.convertSkewedInfo(null));
    assertEquals(null, CatalogToHiveConverter.convertSkewedInfo(null));
  }

  @Test
  public void testConvertSkewedInfoNullFields() {
    SkewedInfo catalogSkewedInfo = new SkewedInfo();
    org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedInfo = CatalogToHiveConverter.convertSkewedInfo(catalogSkewedInfo);
    assertNotNull(hiveSkewedInfo.getSkewedColNames());
    assertNotNull(hiveSkewedInfo.getSkewedColValues());
    assertNotNull(hiveSkewedInfo.getSkewedColValueLocationMaps());
  }

  @Test
  public void testConvertSerdeInfoNullParameter() {
    SerDeInfo serDeInfo = TestObjects.getTestSerdeInfo();
    serDeInfo.setParameters(null);
    assertNotNull(CatalogToHiveConverter.convertSerDeInfo(serDeInfo).getParameters());
  }

  @Test
  public void testFunctionConversion() {
    UserDefinedFunction catalogFunction = TestObjects.getCatalogTestFunction();
    org.apache.hadoop.hive.metastore.api.Function hiveFunction = CatalogToHiveConverter.convertFunction(TEST_DB_NAME, catalogFunction);
    assertEquals(TEST_DB_NAME, hiveFunction.getDbName());
    assertEquals(catalogFunction, HiveToCatalogConverter.convertFunction(hiveFunction));
  }

  @Test
  public void testConvertOrderList() {
    List<org.apache.hadoop.hive.metastore.api.Order> hiveOrderList = ImmutableList.of(TestObjects.getTestOrder());
    List<Order> catalogOrderList = HiveToCatalogConverter.convertOrderList(hiveOrderList);

    assertEquals(hiveOrderList.get(0).getCol(), catalogOrderList.get(0).getColumn());
    assertEquals(hiveOrderList.get(0).getOrder(), catalogOrderList.get(0).getSortOrder().intValue());
  }

  @Test
  public void testConvertOrderListNull() {
    assertNull(HiveToCatalogConverter.convertOrderList(null));
  }

  @Test
  public void testTableMetaConversion() {
    Table catalogTable = TestObjects.getTestTable();
    TableMeta tableMeta = CatalogToHiveConverter.convertTableMeta(catalogTable, TEST_DB_NAME);
    assertEquals(catalogTable.getName(), tableMeta.getTableName());
    assertEquals(TEST_DB_NAME, tableMeta.getDbName());
    assertEquals(catalogTable.getTableType(), tableMeta.getTableType());
  }

  @Test
  public void testTableConversionStorageDescriptorParameterMapNull() {
    Table catalogTable = TestObjects.getTestTable();
    catalogTable.getStorageDescriptor().setParameters(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getSd().getParameters());
    assertTrue(hiveTable.getSd().getParameters().isEmpty());
  }

  @Test
  public void testTableConversionStorageDescriptorBucketColsNull() {
    Table catalogTable = TestObjects.getTestTable();
    catalogTable.getStorageDescriptor().setBucketColumns(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getSd().getBucketCols());
    assertTrue(hiveTable.getSd().getBucketCols().isEmpty());
  }

  @Test
  public void testTableConversionStorageDescriptorSorColsNull() {
    Table catalogTable = TestObjects.getTestTable();
    catalogTable.getStorageDescriptor().setSortColumns(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getSd().getSortCols());
    assertTrue(hiveTable.getSd().getSortCols().isEmpty());
  }

  @Test
  public void testTableConversionWithNullPartitionKeys() {
    Table catalogTable = TestObjects.getTestTable();
    catalogTable.setPartitionKeys(null);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = CatalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertNotNull(hiveTable.getPartitionKeys());
    assertTrue(hiveTable.getPartitionKeys().isEmpty());
  }

  private void verifyReplaceSupportedTypes(String input, String expected) {
    assertEquals(expected, CatalogToHiveConverter.replaceSupportedTypeNames(input));
    assertEquals(expected, CatalogToHiveConverter.replaceSupportedTypeNames(input.toUpperCase()).toLowerCase());
  }
  @Test
  public void testReplaceSupportedTypes() {
    verifyReplaceSupportedTypes("struct<char:char,varchar:varchar," +
            "charcol:char,varcharcol:varchar,char_col:char,varchar_col:varchar>",
    "struct<char:string,varchar:string,charcol:string," +
            "varcharcol:string,char_col:string,varchar_col:string>");

    verifyReplaceSupportedTypes("struct<char:char(19),varchar:varchar(19)," +
            "charcol:char(19),varcharcol:varchar(19),char_col:char(19),varchar_col:varchar(19)>",
            "struct<char:char(19),varchar:varchar(19),charcol:char(19)," +
                    "varcharcol:varchar(19),char_col:char(19),varchar_col:varchar(19)>");

    verifyReplaceSupportedTypes("char", "string");
    verifyReplaceSupportedTypes("varchar", "string");
    verifyReplaceSupportedTypes("char(5)", "char(5)");
    verifyReplaceSupportedTypes("varchar(5)", "varchar(5)");
    verifyReplaceSupportedTypes("array<char>", "array<string>");
    verifyReplaceSupportedTypes("array<varchar>", "array<string>");
    verifyReplaceSupportedTypes("array<char(3)>", "array<char(3)>");
    verifyReplaceSupportedTypes("array<varchar(3)>", "array<varchar(3)>");
    verifyReplaceSupportedTypes("map<char,int>", "map<string,int>");
    verifyReplaceSupportedTypes("map<varchar,int>", "map<string,int>");
    verifyReplaceSupportedTypes("map<char(3),int>", "map<char(3),int>");
    verifyReplaceSupportedTypes("map<varchar(3),int>", "map<varchar(3),int>");
    verifyReplaceSupportedTypes("struct<f1:char,f2:varchar,f3:char(4),f4:varchar(34)",
            "struct<f1:string,f2:string,f3:char(4),f4:varchar(34)");
    verifyReplaceSupportedTypes("array<struct<varchar:char,abc:array<char>>",
            "array<struct<varchar:string,abc:array<string>>");
    verifyReplaceSupportedTypes("struct<charf1:char,f2char:varchar,varcharf3:char(4),f4varchar:varchar(34)",
            "struct<charf1:string,f2char:string,varcharf3:char(4),f4varchar:varchar(34)");
    verifyReplaceSupportedTypes("struct<char_f1:char,f2_char:varchar,varchar_f3:char(4),f4_varchar:varchar(34)",
            "struct<char_f1:string,f2_char:string,varchar_f3:char(4),f4_varchar:varchar(34)");
  }
}
