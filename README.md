receives camunda process-tasks to read/write values from process-io-api

## Write
if a process-variable name starts with the configured write_prefix, it is interpreted as a key by:
- removing the read_prefix
- replacing the configured instance_id_placeholder with the tasks process-instance-id
- replacing the configured process_definition_id_placeholder with the tasks process-definition-id

if the variable value is not a string it used as is. strings are tried to be interpreted as json. if not successful the string is used as is.

example name: `io.read.instance_{{InstanceId}}_foo`

example value: `42`


## Read
if a process-variable name starts with the configured read_prefix, it is interpreted as output-variable name by removing the prefix.
the variable value is interpreted as value-key by:
- replacing the configured instance_id_placeholder with the tasks process-instance-id
- replacing the configured process_definition_id_placeholder with the tasks process-definition-id

if the instance_id_placeholder has been used, the stored value will be annotated with the process-definition-id and process-instance-id  

if the process_definition_id_placeholder has been used, the stored value will be annotated with the process-definition-id

these annotations are needed to later delete instance/deployment dependent values if the deployment/instance is deleted.

example name: `io.write.result`

example value: `instance_{{InstanceId}}_foo`


## Default
it is possible to define a default value for a read:

example name: `io.default.instance_{{InstanceId}}_foo`

example value: `0`