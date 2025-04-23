<!--
    Copyright 2025 Tabs Data Inc.
-->

# Function I/O Yaml structure

### request.yaml,

```
class: ephemeral
worker: function
action: start
arguments: []
get-states:
  - state-type: map
    state-key: mount_options
      - prefixes: TD_1_
                  TD_2_
callback: !Http
  url: http://127.0.0.1:2457/function_run/{function_run_id}
  method: POST
  headers: {}
  body: true
context: !V1
  info:                                                                # metadata about the function
    collection_id: {c_id}                                            
    collection: {c}                                   
    function_id: {f_id}                                                  
    function_version_id: {fv_id}                                                  
    function: {f}                                                  
    function_run_id: {fr_id}                                                  
    function_bundle:
      uri: file:///../.tdserver/default/c/{c_id}/f/{f_id}/BUNDLE.tgz   # URI to the function bundle (file://, s3://, azure://, gcp://)
      env_prefix: TD_1_                                                # Prefix env vars with cloud configs/credentials to fetch the function bundle
    triggered_on: {int}                                         
    transaction_id: {t_id}                                         
    execution_id: {e_id}
    execution_name: {e}
  system_input:                                                        # array with all system input tables
  - !Table
    name: td-initial-values           
    collection_id: {c_id}                                            
    collection: {c}                                      
    table_id: {t_id}    
    table_version_id: {tv_id}    
    table_data_version_id: {tdv_id}                                                                                                
    location: null
    table_pos: -1                                                      # position in the table array (always < 0)
    version_pos: 0                                                     # position in the versions array
  input:                                                               # array with all input tables
  - !Table
    name: emails                                                      
    collection_id: {c_id}                                            
    collection: {c}              
    table_id: {t_id}                                                              
    table_version_id: {tv_id}                                                              
    table_data_version_id: {tdv_id}                                                              
    location:                                                          
      uri: file:///../.tdserver/default/c/{c_id}/d/{tdv_id}/t/{t_id}/{tv_id}.t
      env_prefix: TD_2_   
    table_pos: 0                                                       # position in the table array (always >= 0)
    version_pos: 0                                                     # position in the versions array
  system_output:                                                       # array with all system output tables
  - !Table
    name: td-initial-values
    collection_id: {c_id}                                            
    collection: {c}              
    table_id: {t_id}   
    table_version_id: {tv_id}   
    table_data_version_id: {tdv_id}   
    location:                                                          
      uri: file:///../.tdserver/default/c/{c_id}/d/{tdv_id}/t/{t_id}/{tv_id}.t   # URI to the table data (file://, s3://, azure://, gcp://)
      env_prefix: TD_2_                                                          # Prefix env vars with cloud configs/credentials to store the table data
    table_pos: -1                                                                # position in the table array (always < 0)
  output:                                                                        # array with all output tables
  - !Table
    name: users     
    collection_id: {c_id}                                            
    collection: {c}            
    table_id: {t_id}  
    table_version_id: {tv_id}                                           
    table_data_version_id: {tdv_id}                                           
    location:
      uri: file:///../.tdserver/default/c/{c_id}/d/{tdv_id}/t/{t_id}/{tv_id}.t    # URI to the table data (file://, s3://, azure://, gcp://)
      env_prefix: TD_1_                                                           # Prefix env vars with cloud configs/credentials to store the table data
    table_pos: 0                                                                  # position in the table array (always >= 0)
```

### response.yaml

```
id: id
class: ephemeral
worker: function
action: Notify
start: epoch_millis
end: epoch_millis
status: Done
execution: 1
limit: 5
error: null,
context:
  !V1
  system_output:
  - !Data                      # indicates that the table was written with new data as part of the function run
    table:
      name: td-initial-values    # table name
  output:
  - !NoData                    # indicates that the table was not written with new data as part of the function run
    table:
      name: users
```
