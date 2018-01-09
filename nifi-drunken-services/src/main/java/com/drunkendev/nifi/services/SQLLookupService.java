/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.drunkendev.nifi.services;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import static java.util.Collections.singleton;


@Tags({"sql", "lookup"})
@CapabilityDescription("SQL implementation of a LookupService that returns a Record as its result.")
public class SQLLookupService extends AbstractControllerService implements LookupService<Record> {

    static final PropertyDescriptor PROP_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    private static final String KEY = "sql";
    private static final Set<String> KEYS = singleton(KEY);
    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_DBCP_SERVICE);
        PROPERTIES = Collections.unmodifiableList(props);
    }

    private DBCPService dbcp;
    private ComponentLog log;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    /**
     * @param   cc
     *          the configuration context
     * @throws  InitializationException
     *          if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(ConfigurationContext cc) throws InitializationException {
        this.dbcp = cc.getProperty(PROP_DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        this.log = getLogger();
    }

    @OnDisabled
    public void shutdown() {
    }

    @Override
    public Optional<Record> lookup(Map<String, String> coordinates) throws LookupFailureException {
        if (!coordinates.containsKey(KEY)) {
            throw new LookupFailureException("Required key " + KEY + " was not present in the lookup.");
        }

        String query = coordinates.get(KEY);

        try (Connection con = dbcp.getConnection();
             Statement stmt = con.createStatement()) {

            log.debug("Executing query {}", new Object[]{query});
            if (stmt.execute(query)) {
                ResultSet rs = stmt.getResultSet();
                ResultSetMetaData meta = rs.getMetaData();
                int count = meta.getColumnCount();

                if (rs.next()) {
                    List<RecordField> fields = new ArrayList<>();
                    Map<String, Object> res = new HashMap<>();

                    for (int i = 1; i <= count; i++) {
                        String col = meta.getColumnName(i);
                        try {
                            int t = meta.getColumnType(i);
                            fields.add(new RecordField(col, getType(t)));
                            Object val = getValue(rs, t, i);
                            res.put(col, val);
                        } catch (UnsupportedOperationException ex) {
                            log.warn("Column " + col + " skipped: " + ex.getMessage());
                        }
                    }
                    return Optional.of(new MapRecord(new SimpleRecordSchema(fields), res));
                }
            }
            return Optional.empty();
        } catch (Exception ex) {
            log.error("Unable to execute SQL select query: " + ex.getMessage(), ex);
            throw new LookupFailureException("Unable to execute SQL select query: " + ex.getMessage(), ex);
        }
    }

    @Override
    public Class getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return KEYS;
    }

    private DataType getType(int t) {
        switch (t) {
            case Types.ARRAY:
                return RecordFieldType.ARRAY.getDataType();
            case Types.BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case Types.DATE:
                return RecordFieldType.DATE.getDataType();
            case Types.DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case Types.FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return RecordFieldType.INT.getDataType();
            case Types.TIME: return RecordFieldType.TIME.getDataType();
            case Types.TIMESTAMP:
                return RecordFieldType.TIMESTAMP.getDataType();
            case Types.CHAR:
            case Types.VARCHAR:
                return RecordFieldType.STRING.getDataType();
        }
        throw new UnsupportedOperationException("SQL type " + t + " not supported.");
    }

    private Object getValue(ResultSet rs, int t, int col) throws SQLException {
        switch (t) {
            case Types.ARRAY:
                return rs.getArray(col);
            case Types.BOOLEAN:
                return rs.getBoolean(col);
            case Types.DATE:
                return rs.getDate(col);
            case Types.DOUBLE:
                return rs.getDouble(col);
            case Types.FLOAT:
                return rs.getFloat(col);
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return rs.getInt(col);
            case Types.TIME: return RecordFieldType.TIME.getDataType();
            case Types.TIMESTAMP:
                return rs.getTimestamp(col);
            case Types.CHAR:
            case Types.VARCHAR:
                return rs.getString(col);
        }
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
