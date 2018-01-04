/*
 * UpdateAttributesFromSQL.java    Jan 3 2018, 18:27
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;


/**
 * Processor that will execute an SQL query and set attributes found from the
 * first record against the current flow file.
 *
 * @author  Brett Ryan
 */
@Tags({"sql", "select", "jdbc", "query", "database", "attribute"})
@CapabilityDescription(
        "Adds FlowFile attributes found by the provided select query. " +
        "Each attribute set will have a name from all columns in the query result.")
@WritesAttributes({
    @WritesAttribute(attribute = "updateattributesfromsql.query.duration",
                     description = "Duration of the query in milliseconds")
    ,
    @WritesAttribute(attribute = "updateattributesfromsql.query.count",
                     description = "Count of properties set from SQL query.")
})
public class UpdateAttributesFromSQL extends AbstractProcessor {

    static final PropertyDescriptor PROP_ATTR_NAMES = new PropertyDescriptor.Builder()
            .name("Attributes Names")
            .description("Optional names for attributes to set in the order found in the result set seperated by comma or space.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    static final PropertyDescriptor PROP_ATTR_PREFIX = new PropertyDescriptor.Builder()
            .name("Attribute Prefix")
            .description("Prefixes all discovered attributes with this value.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    static final PropertyDescriptor PROP_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    static final PropertyDescriptor PROP_FAIL_ON_NO_RECORDS = new PropertyDescriptor.Builder()
            .name("Fail on No Records")
            .description("If no records are found this process will redirect to the failed relationship.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .allowableValues("true", "false")
            .expressionLanguageSupported(false)
            .build();
    static final PropertyDescriptor PROP_LOWER_CASE_NAMES = new PropertyDescriptor.Builder()
            .name("Lower Case Attribute Names")
            .description("Convert all attributes to lower case.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .allowableValues("true", "false")
            .expressionLanguageSupported(false)
            .build();
    static final PropertyDescriptor PROP_OVERWRITE_EXISTING_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Overwrite existing attributes")
            .description("If an attribute exists it will be overwritten when this is set to true.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .allowableValues("true", "false")
            .expressionLanguageSupported(false)
            .build();
    static final PropertyDescriptor PROP_QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query, " +
                         " zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();
    static final PropertyDescriptor PROP_SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("SQL Select Query")
            .description("The SQL select query to execute.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles with attributes successfully set are routed to this relationship.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles with failing SQL lookups are routed to this relationship.")
            .build();

    static final String ATTR_DURATION = "updateattributesfromsql.query.duration";
    static final String ATTR_PROP_COUNT = "updateattributesfromsql.query.count";

    private static final List<PropertyDescriptor> DESCRIPTORS;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        List<PropertyDescriptor> d = new ArrayList<>();
        d.add(PROP_ATTR_NAMES);
        d.add(PROP_ATTR_PREFIX);
        d.add(PROP_DBCP_SERVICE);
        d.add(PROP_FAIL_ON_NO_RECORDS);
        d.add(PROP_LOWER_CASE_NAMES);
        d.add(PROP_OVERWRITE_EXISTING_ATTRIBUTES);
        d.add(PROP_QUERY_TIMEOUT);
        d.add(PROP_SQL_SELECT_QUERY);
        DESCRIPTORS = Collections.unmodifiableList(d);

        Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(r);
    }

    private ComponentLog log;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        this.log = getLogger();
    }

    @Override
    public void onTrigger(ProcessContext context,
                          ProcessSession session)
            throws ProcessException {
        FlowFile ff = session.get();
        if (ff == null) {
            return;
        }

        DBCPService dbcp = context.getProperty(PROP_DBCP_SERVICE).asControllerService(DBCPService.class);
        Integer timeout = context.getProperty(PROP_QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        String query = context.getProperty(PROP_SQL_SELECT_QUERY).evaluateAttributeExpressions(ff).getValue();
        String prefix = context.getProperty(PROP_ATTR_PREFIX).isSet() ?
                context.getProperty(PROP_ATTR_PREFIX).evaluateAttributeExpressions(ff).getValue() :
                "";
        boolean overwrite = context.getProperty(PROP_OVERWRITE_EXISTING_ATTRIBUTES).isSet() &&
                            context.getProperty(PROP_OVERWRITE_EXISTING_ATTRIBUTES).asBoolean();
        boolean toLower = context.getProperty(PROP_LOWER_CASE_NAMES).isSet() &&
                          context.getProperty(PROP_LOWER_CASE_NAMES).asBoolean();
        boolean failNone = context.getProperty(PROP_FAIL_ON_NO_RECORDS).isSet() &&
                           context.getProperty(PROP_FAIL_ON_NO_RECORDS).asBoolean();

        String[] attrNames;
        if (context.getProperty(PROP_ATTR_NAMES).isSet()) {
            String attrNameVal = context.getProperty(PROP_ATTR_NAMES).evaluateAttributeExpressions(ff).getValue();
            attrNames = attrNameVal.trim().split("[, ]+");
        } else {
            attrNames = new String[0];
        }

        Map<String, String> attributes = new HashMap<>(ff.getAttributes());

        StopWatch stopWatch = new StopWatch(true);

        try (Connection con = dbcp.getConnection();
             Statement stmt = con.createStatement()) {
            stmt.setQueryTimeout(timeout);

            int count = 0;
            log.debug("Executing query {}", new Object[]{query});
            if (stmt.execute(query)) {
                ResultSet rs = stmt.getResultSet();
                ResultSetMetaData meta = rs.getMetaData();
                count = meta.getColumnCount();
                if (rs.next()) {
                    for (int i = 0; i < count; i++) {
                        String colName = prefix +
                                         (attrNames.length > i ?
                                                 attrNames[i] :
                                                 meta.getColumnName(i + 1));
                        colName = toLower ? colName.toLowerCase() : colName;
                        if (overwrite || !attributes.containsKey(colName)) {
                            attributes.put(colName, rs.getString(i + 1));
                        }
                    }
                } else if (failNone) {
                    session.transfer(ff, REL_FAILURE);
                    return;
                }
                ff = session.putAllAttributes(ff, attributes);
            }

            long duration = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            ff = session.putAttribute(ff, ATTR_DURATION, String.valueOf(duration));
            ff = session.putAttribute(ff, ATTR_PROP_COUNT, String.valueOf(count));
            session.getProvenanceReporter().modifyContent(ff, "Got " + count + " attributes", duration);

            session.transfer(ff, REL_SUCCESS);
        } catch (Exception ex) {
            log.error("Unable to execute SQL select query {} for {} due to {}; routing to failure",
                      new Object[]{query, ff, ex});
            ff = session.penalize(ff);
            session.transfer(ff, REL_FAILURE);
        }
    }

}
