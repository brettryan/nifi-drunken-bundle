/*
 * LookupAttributeFromSQLTest.java    Jan 3 2018, 18:27
 *
 * Copyright 2018 Apache NiFi Project.
 *
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

package com.drunkendev.nifi.processors;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.drunkendev.nifi.processors.LookupAttributeFromSQL.ATTR_DURATION;
import static com.drunkendev.nifi.processors.LookupAttributeFromSQL.ATTR_PROP_COUNT;
import static com.drunkendev.nifi.processors.LookupAttributeFromSQL.PROP_ATTR_NAMES;
import static com.drunkendev.nifi.processors.LookupAttributeFromSQL.PROP_SQL_SELECT_QUERY;
import static com.drunkendev.nifi.processors.LookupAttributeFromSQL.REL_FAILURE;
import static com.drunkendev.nifi.processors.LookupAttributeFromSQL.REL_SUCCESS;


public class LookupAttributeFromSQLTest {

    private static final Logger LOG = LoggerFactory.getLogger(LookupAttributeFromSQLTest.class);

    private final static String DB_LOCATION = "target/db";

    private TestRunner runner;

    @BeforeClass
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();
    }

    @Before
    public void init() throws InitializationException, SQLException {
        DBCPService dbcp = new DBCPServiceSimpleImpl();
        Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(LookupAttributeFromSQL.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.setProperty(LookupAttributeFromSQL.PROP_DBCP_SERVICE, "dbcp");
        runner.assertValid(dbcp);
        runner.enableControllerService(dbcp);

        // load test data to database
        try (Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
             Statement st = con.createStatement()) {

            try {
                st.executeUpdate("drop table test_data");
            } catch (SQLException ex) {
            }

            st.executeUpdate("create table test_data (id integer, name varchar(100), code integer)");

            st.executeUpdate("insert into test_data (id, name, code) values" +
                             "(1, 'test', 100)," +
                             "(2, 'Jon Doe', 200)," +
                             "(3, 'Jane Doe', 300)," +
                             "(4, 'Some Name', null)," +
                             "(4, null, 400)," +
                             "(4, null, null)");
        }

        LOG.info("test data loaded");
    }


    @Test
    public void testProcessor() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setIncomingConnection(true);
        runner.setProperty(PROP_SQL_SELECT_QUERY, "select * from test_data where id = 1");
        runner.assertValid();

        runner.enqueue("".getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_DURATION);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_PROP_COUNT);

        List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        flowfiles.get(0).assertAttributeEquals("id", "1");
        flowfiles.get(0).assertAttributeEquals("name", "test");
        flowfiles.get(0).assertAttributeEquals("code", "100");
    }

    @Test
    public void testProcessor2() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setIncomingConnection(true);
        runner.setProperty(PROP_SQL_SELECT_QUERY, "select * from test_data where id = 2");
        runner.setProperty(LookupAttributeFromSQL.PROP_LOWER_CASE_NAMES, "false");
        runner.assertValid();

        runner.enqueue("".getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_DURATION);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_PROP_COUNT);

        List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        flowfiles.get(0).assertAttributeNotExists("id");
        flowfiles.get(0).assertAttributeEquals("ID", "2");
        flowfiles.get(0).assertAttributeEquals("NAME", "Jon Doe");
        flowfiles.get(0).assertAttributeEquals("CODE", "200");
    }

    @Test
    public void testProcessor3() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setIncomingConnection(true);
        runner.setProperty(PROP_SQL_SELECT_QUERY, "select * from test_data where id = 3");
        runner.setProperty(LookupAttributeFromSQL.PROP_OVERWRITE_EXISTING_ATTRIBUTES, "false");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();
        props.put("code", "xyz");
        runner.enqueue("".getBytes(), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_DURATION);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_PROP_COUNT);

        List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        flowfiles.get(0).assertAttributeEquals("id", "3");
        flowfiles.get(0).assertAttributeEquals("name", "Jane Doe");
        flowfiles.get(0).assertAttributeEquals("code", "xyz");
    }


    @Test
    public void testProcessor4() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setIncomingConnection(true);
        runner.setProperty(PROP_SQL_SELECT_QUERY, "select * from test_data where id = 3");
        runner.setProperty(LookupAttributeFromSQL.PROP_ATTR_PREFIX, "my-prefix-");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();
        runner.enqueue("".getBytes(), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_DURATION);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_PROP_COUNT);

        List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        flowfiles.get(0).assertAttributeEquals("my-prefix-id", "3");
        flowfiles.get(0).assertAttributeEquals("my-prefix-name", "Jane Doe");
        flowfiles.get(0).assertAttributeEquals("my-prefix-code", "300");
    }

    @Test
    public void testProcessor5() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setIncomingConnection(true);
        runner.setProperty(PROP_SQL_SELECT_QUERY, "select * from test_data where id = -1");
        runner.setProperty(LookupAttributeFromSQL.PROP_FAIL_ON_NO_RECORDS, "true");
        runner.assertValid();

        runner.enqueue("".getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testProcessor6() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setIncomingConnection(true);
        runner.setProperty(PROP_SQL_SELECT_QUERY, "select * from test_data where id = ${test_id}");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();
        props.put("test_id", "3");
        runner.enqueue("".getBytes(), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_DURATION);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_PROP_COUNT);

        List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        flowfiles.get(0).assertAttributeEquals("id", "3");
        flowfiles.get(0).assertAttributeEquals("name", "Jane Doe");
        flowfiles.get(0).assertAttributeEquals("code", "300");
    }

    @Test
    public void testProcessor7() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setIncomingConnection(true);
        runner.setProperty(PROP_SQL_SELECT_QUERY, "select ID, NAME, CODE from test_data where id = 3");
        runner.setProperty(PROP_ATTR_NAMES, " p1 , p2  p3  ");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();
        props.put("test_id", "3");
        runner.enqueue("".getBytes(), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_DURATION);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_PROP_COUNT);

        List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        flowfiles.get(0).assertAttributeEquals("p1", "3");
        flowfiles.get(0).assertAttributeEquals("p2", "Jane Doe");
        flowfiles.get(0).assertAttributeEquals("p3", "300");
    }


    @Test
    public void testProcessor8() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setIncomingConnection(true);
        runner.setProperty(PROP_SQL_SELECT_QUERY, "select ID, NAME, CODE from test_data where id = 3");
        runner.setProperty(PROP_ATTR_NAMES, "p1,p2");
        runner.assertValid();

        Map<String, String> props = new HashMap<>();
        props.put("test_id", "3");
        runner.enqueue("".getBytes(), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_DURATION);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, ATTR_PROP_COUNT);

        List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        flowfiles.get(0).assertAttributeEquals("p1", "3");
        flowfiles.get(0).assertAttributeEquals("p2", "Jane Doe");
        flowfiles.get(0).assertAttributeEquals("code", "300");
    }



    /**
     * Simple implementation taken from ExecuteSQL processor testing.
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }

    }

}
