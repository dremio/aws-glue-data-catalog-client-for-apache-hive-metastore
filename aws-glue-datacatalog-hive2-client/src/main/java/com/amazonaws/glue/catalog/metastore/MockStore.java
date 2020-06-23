package com.amazonaws.glue.catalog.metastore;

import com.google.common.collect.Maps;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Mock store containing list of tables
 */
public class MockStore {

    private List<MockTable> tables;

    public List<MockTable> getTables() {
        return tables;
    }

    public void setTables(List<MockTable> tables) {
        this.tables = tables;
    }

    public MockStore() {
        tables = Collections.emptyList();
    }

    public static MockStore fromJson(String json) {
        MockStore mockStore = new MockStore();
        List<MockStore.MockTable> mockTables = new ArrayList<>();
        try {
            JSONObject storeObj = new JSONObject(json);
            if (!storeObj.has("tables")) {
                return mockStore;
            }

            JSONArray tablesObj = storeObj.getJSONArray("tables");

            for (int i = 0; i < tablesObj.length(); i++) {
                JSONObject tableObj = tablesObj.getJSONObject(i);
                mockTables.add(MockTable.fromJson(tableObj));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        mockStore.setTables(mockTables);
        return mockStore;
    }

    /**
     * Mock hive table
     */
    public static class MockTable {
        // database name
        private String dbname = "";

        // table name
        private String tablename = "";

        // table schema
        private List<MockFieldSchema> fields;

        // table location
        private String location;

        // input format, output format and serde library
        private String inputformat;
        private String outputformat;
        private String serializationlib;

        // serde properties like delimiter
        private Map<String, String> serdeprops;

        // file size, record count
        private String sizekey;
        private String recordcount;

        // table type like EXTERNAL_TABLE
        private String tabletype;

        public MockTable() {
            dbname = "";
            tablename = "";
            fields = Collections.emptyList();
            location = "";
            serdeprops = Maps.<String, String>newHashMap();
            sizekey = "";
            recordcount = "";
            tabletype = "";
        }

        public String getDbname() {
            return dbname;
        }

        public void setDbname(String dbname) {
            this.dbname = dbname;
        }

        public String getTablename() {
            return tablename;
        }

        public void setTablename(String tablename) {
            this.tablename = tablename;
        }

        public List<MockFieldSchema> getFields() {
            return fields;
        }

        public void setFields(List<MockFieldSchema> fields) {
            this.fields = fields;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public String getInputformat() {
            return inputformat;
        }

        public void setInputformat(String inputformat) {
            this.inputformat = inputformat;
        }

        public String getOutputformat() {
            return outputformat;
        }

        public void setOutputformat(String outputformat) {
            this.outputformat = outputformat;
        }

        public String getSerializationlib() {
            return serializationlib;
        }

        public void setSerializationlib(String serializationlib) {
            this.serializationlib = serializationlib;
        }

        public Map<String, String> getSerdeprops() {
            return serdeprops;
        }

        public void setSerdeprops(Map<String, String> serdeprops) {
            this.serdeprops = serdeprops;
        }

        public String getSizekey() {
            return sizekey;
        }

        public void setSizekey(String sizekey) {
            this.sizekey = sizekey;
        }

        public String getRecordcount() {
            return recordcount;
        }

        public void setRecordcount(String recordcount) {
            this.recordcount = recordcount;
        }

        public String getTabletype() {
            return tabletype;
        }

        public void setTabletype(String tabletype) {
            this.tabletype = tabletype;
        }

        public static MockTable fromJson(JSONObject tableObj) throws JSONException {
            MockTable table = new MockTable();
            if (tableObj.has("dbname")) {
                table.setDbname(tableObj.getString("dbname"));
            }

            if (tableObj.has("tablename")) {
                table.setTablename(tableObj.getString("tablename"));
            }

            if (tableObj.has("location")) {
                table.setLocation(tableObj.getString("location"));
            }

            if (tableObj.has("inputformat")) {
                table.setInputformat(tableObj.getString("inputformat"));
            }

            if (tableObj.has("outputformat")) {
                table.setOutputformat(tableObj.getString("outputformat"));
            }

            if (tableObj.has("serializationlib")) {
                table.setSerializationlib(tableObj.getString("serializationlib"));
            }

            if (tableObj.has("sizekey")) {
                table.setSizekey(tableObj.getString("sizekey"));
            }

            if (tableObj.has("recordcount")) {
                table.setRecordcount(tableObj.getString("recordcount"));
            }

            if (tableObj.has("tabletype")) {
                table.setTabletype(tableObj.getString("tabletype"));
            }

            if (tableObj.has("fields")) {
                JSONArray fieldsObj = tableObj.getJSONArray("fields");
                List<MockStore.MockFieldSchema> mockFields = new ArrayList<>();
                for (int f = 0; f < fieldsObj.length(); f++) {
                    JSONObject fieldSchemaObj = fieldsObj.getJSONObject(f);
                    mockFields.add(MockFieldSchema.fromJson(fieldSchemaObj));
                }
                table.setFields(mockFields);
            }

            if (tableObj.has("serdeprops")) {
                JSONArray serdePropsObj = tableObj.getJSONArray("serdeprops");
                Map<String, String> serdeProps = new HashMap<>();
                for (int prop = 0; prop < serdePropsObj.length(); prop++) {
                    JSONObject serdeProp = serdePropsObj.getJSONObject(prop);
                    Iterator<String> keyIter = serdeProp.keys();
                    while (keyIter.hasNext()) {
                        String keyName = keyIter.next();
                        serdeProps.put(keyName, serdeProp.getString(keyName));
                    }
                }
                table.setSerdeprops(serdeProps);
            }

            return table;
        }
    }

    /**
     * Mock field of table schema
     */
    public static class MockFieldSchema {
        private String type;
        private String name;
        private List<MockFieldSchema> children;

        public MockFieldSchema() {
            type = "";
            name = "";
            children = Collections.emptyList();
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<MockFieldSchema> getChildren() {
            return children;
        }

        public void setChildren(List<MockFieldSchema> children) {
            this.children = children;
        }

        public static MockFieldSchema fromJson(JSONObject fieldSchemaObj) throws JSONException {
            MockStore.MockFieldSchema fieldSchema = new MockStore.MockFieldSchema();
            if (fieldSchemaObj.has("type")) {
                fieldSchema.setType(fieldSchemaObj.getString("type"));
            }

            if (fieldSchemaObj.has("name")) {
                fieldSchema.setName(fieldSchemaObj.getString("name"));
            }

            if (fieldSchemaObj.has("children")) {
                JSONArray childFieldObjs = fieldSchemaObj.getJSONArray("children");
                List<MockStore.MockFieldSchema> childFileds = new ArrayList<>();
                for (int child = 0; child < childFieldObjs.length(); child++) {
                    JSONObject childFieldObj = childFieldObjs.getJSONObject(child);
                    childFileds.add(MockFieldSchema.fromJson(childFieldObj));
                }
                fieldSchema.setChildren(childFileds);
            }

            return fieldSchema;
        }
    }
}
