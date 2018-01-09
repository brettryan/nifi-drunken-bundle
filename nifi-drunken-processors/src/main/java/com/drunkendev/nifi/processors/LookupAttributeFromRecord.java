/*
 * LookupAttributeFromRecord.java    Jan 8 2018, 07:46
 *
 * Copyright 2018 Drunken Dev.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.drunkendev.nifi.processors;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.Record;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;


/**
 *
 * @author  Brett Ryan
 */
public class LookupAttributeFromRecord extends AbstractProcessor {

    static final PropertyDescriptor PROP_ATTR_PREFIX = new PropertyDescriptor.Builder()
            .name("Attribute Prefix")
            .description("Prefixes all discovered attributes with this value.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    static final PropertyDescriptor PROP_FAIL_ON_NO_RECORDS = new PropertyDescriptor.Builder()
            .name("Fail on No Records")
            .description("If no records are found this process will redirect to the failed relationship.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    static final PropertyDescriptor PROP_KEY_PREFIX = new PropertyDescriptor.Builder()
            .name("Key Prefix")
            .description("If an attribute starts with this key it will be passed to the lookup as the lookup key.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("key.")
            .build();
    static final PropertyDescriptor PROP_LOWER_CASE_NAMES = new PropertyDescriptor.Builder()
            .name("Lower Case Attribute Names")
            .description("Convert all attributes to lower case.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();
    static final PropertyDescriptor PROP_OVERWRITE_EXISTING_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Overwrite existing attributes")
            .description("If an attribute exists it will be overwritten when this is set to true.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();
    static final PropertyDescriptor PROP_LOOKUP_SERVICE = new PropertyDescriptor.Builder()
            .name("Record Lookup Service")
            .description("The Controller Service that will be used to lookup the record.")
            .required(true)
            .identifiesControllerService(LookupService.class)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles with attributes successfully set are routed to this relationship.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles with failing SQL lookups are routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        List<PropertyDescriptor> d = new ArrayList<>();
        d.add(PROP_ATTR_PREFIX);
        d.add(PROP_FAIL_ON_NO_RECORDS);
        d.add(PROP_KEY_PREFIX);
        d.add(PROP_LOOKUP_SERVICE);
        d.add(PROP_LOWER_CASE_NAMES);
        d.add(PROP_OVERWRITE_EXISTING_ATTRIBUTES);
        DESCRIPTORS = Collections.unmodifiableList(d);

        Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(r);
    }

    private ComponentLog log;
    private Map<PropertyDescriptor, PropertyValue> dynamicProperties;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        this.log = getLogger();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext vc) {
        log.debug("Validating");

        List<ValidationResult> res = new ArrayList<>();

        String keyPrefix = vc.getProperty(PROP_KEY_PREFIX).getValue();

        LookupService<Record> lookupService = vc.getProperty(PROP_LOOKUP_SERVICE)
                .asControllerService(LookupService.class);

        Set<String> reqKeys = lookupService.getRequiredKeys().stream()
                .map(n -> keyPrefix + n)
                .collect(toSet());

        Set<String> dynProps = vc.getProperties().keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .map(n -> n.getName())
                .collect(toSet());

        if (dynProps.isEmpty()) {
            res.add(new ValidationResult.Builder()
                    .subject("No keys for lookup service present.")
                    .valid(false)
                    .explanation("You must specify at least one key property that starts with " + PROP_KEY_PREFIX.getName())
                    .build());
        }

        if (!reqKeys.isEmpty() && !dynProps.containsAll(reqKeys)) {
            res.add(new ValidationResult.Builder()
                    .subject("Required keys for lookup service not present.")
                    .valid(false)
                    .explanation("The lookup service for '" + PROP_LOOKUP_SERVICE.getName() + "' requires that the following keys be present -> " +
                                 reqKeys.stream().collect(joining(", ")) +
                                 ". You are missing -> " +
                                 reqKeys.stream().filter(n -> !dynProps.contains(n)).collect(joining(", ")))
                    .build());
        }

        return res;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        log.debug("Configuring dynamic properties");
        this.dynamicProperties = context.getProperties().keySet().stream()
                .filter(n -> n.isDynamic())
                .map(n -> new ImmutablePair<>(n, context.getProperty(n)))
                .collect(toMap(Pair::getLeft, Pair::getRight));
    }

    @Override
    public void onTrigger(ProcessContext pc, ProcessSession ps) throws ProcessException {
        final FlowFile ff = ps.get();
        if (ff == null) {
            return;
        }

        log.debug("Triggering flow file");

        // todo: handle other return types.
        LookupService<Record> lookupService = pc.getProperty(PROP_LOOKUP_SERVICE)
                .asControllerService(LookupService.class);
        String keyPrefix = pc.getProperty(PROP_KEY_PREFIX).getValue();
        String attrPrefix = pc.getProperty(PROP_ATTR_PREFIX).isSet() ?
                pc.getProperty(PROP_ATTR_PREFIX).getValue() : "";
        boolean overwrite = pc.getProperty(PROP_OVERWRITE_EXISTING_ATTRIBUTES).isSet() &&
                            pc.getProperty(PROP_OVERWRITE_EXISTING_ATTRIBUTES).asBoolean();
        boolean toLower = pc.getProperty(PROP_LOWER_CASE_NAMES).isSet() &&
                          pc.getProperty(PROP_LOWER_CASE_NAMES).asBoolean();
        boolean failNone = pc.getProperty(PROP_FAIL_ON_NO_RECORDS).isSet() &&
                           pc.getProperty(PROP_FAIL_ON_NO_RECORDS).asBoolean();

        try {
            if (dynamicProperties == null) {
                log.error("Dynamic properties is null");
                ps.transfer(ps.penalize(ff), REL_FAILURE);
                return;
            }

            Map<Boolean, Map<String, String>> processConfig = dynamicProperties.entrySet().stream()
                    .map(n -> new ImmutablePair<>(
                    n.getKey().getName(),
                    n.getValue()
                            .evaluateAttributeExpressions(ff)
                            .getValue()))
                    .collect(partitioningBy(n -> n.getKey().startsWith(keyPrefix),
                                            toMap(n -> removePrefix(n.getLeft(), keyPrefix),
                                                  n -> n.getRight())));

            dynamicProperties.forEach((k, v) -> log.debug(" -- ENT: " + k + " = " + v));

            Optional<Record> res = lookupService.lookup(processConfig.getOrDefault(true, emptyMap()));

            Map<String, String> attributes = new HashMap<>(ff.getAttributes());
            if (res.isPresent()) {
                Record record = res.get();

                if (processConfig.containsKey(false) && !processConfig.get(false).isEmpty()) {
                    log.debug("Record attributes specified.");
                    processConfig.get(false).forEach((k, v) -> {
                        log.debug("- Reading field " + v + " into attribute " + k);
                        String colName = (attrPrefix == null ? "" : attrPrefix) + k;
                        colName = toLower ? colName.toLowerCase() : colName;
                        if (overwrite || !attributes.containsKey(colName)) {
                            attributes.put(k, record.getAsString(v));
                        }
                    });
                } else {
                    log.debug("No attributes specified, setting all.");
                    record.getSchema().getFieldNames().forEach(k -> {
                        log.debug("- Reading field " + k);
                        String colName = (attrPrefix == null ? "" : attrPrefix) + k;
                        colName = toLower ? colName.toLowerCase() : colName;
                        if (overwrite || !attributes.containsKey(colName)) {
                            attributes.put(colName, record.getAsString(k));
                        }
                    });
                }
            } else if (failNone) {
                log.error("Failing because no record was found.");
                ps.transfer(ff, REL_FAILURE);
                return;
            }
            ps.transfer(ps.putAllAttributes(ff, attributes), REL_SUCCESS);
        } catch (LookupFailureException ex) {
            log.error("Failing because of a lookup exception - {}", new Object[]{ex.getMessage()}, ex);
            ps.transfer(ps.penalize(ff), REL_FAILURE);
        }
    }

    private String removePrefix(String left, String keyPrefix) {
        return left.startsWith(keyPrefix) ?
                left.substring(keyPrefix.length()) :
                left;
    }

}
