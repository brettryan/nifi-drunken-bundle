
# NiFi Drunken Dev Bundle

This NiFi bundle serves to add enrichment support to FlowFiles by adding
attributes.

Presently one process is available which will execute SQL against the FlowFile
adding the values from the first record returned as properties to the FlowFile
with the property names being that of the column names identified in the
`ResultSet`.


## Example

Given a table with the following

```
create table if not exists test_table (
  id    identity primary key,
  some_name   varchar,
  some_id     int
);
insert into test_table (some_name, some_id) values ('test val', 3);
```

We could define a processor as follows:

[[https://github.com/brettryan/nifi-drunken-bundle/blob/master/doc/img/flow-example.png|alt=flowexample]]

[[https://github.com/brettryan/nifi-drunken-bundle/blob/master/doc/img/processor-details.png|alt=processor-details]]

This would result in the following flow file attributes being applied.

[[https://github.com/brettryan/nifi-drunken-bundle/blob/master/doc/img/flow-file-attributes.png|alt=flow-file-attributes]]

