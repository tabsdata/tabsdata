<!--
    Copyright 2024 Tabs Data Inc.
-->

# Dataset Function I/O Yaml structure

## Importer run

### request.yaml,

```
class: ephemeral
worker: dataset
action: start
arguments: []
callback: !Http
  url: http://127.0.0.1:2457/data_version/069OU3FP8TQR7D53RAJO5Q84A0
  method: POST
  headers: {}
  body: true
context: !V1
  info:                                                                # metadata about the dataset function
    dataset: td:///eu/users                                            
    dataset_id: td:///eu_id/users_id                                   
    function_id: f_id                                                  
    function_bundle:
      uri: file:///../.tdserver/default/s/eu_id/d/users_id/f/f_id.f    # URI to the dataset function bundle (file://, s3://, azure://, gcp://)
      env_prefix: null                                                 # Prefix env vars with cloud configs/credentials to fetch the function bundle
    dataset_data_version: v_id                                         
    execution_plan_id: ep_id                                           
    execution_plan_dataset: td://eu/users                              
    execution_plan_dataset_id: td:///eu_id/users_id                    
  system_input:                                                        # array with all system input tables
  - !Table
    name: td-initial-values                                           
    table: td:///eu/users/td-initial-values@HEAD~1
    table_id: td:///eu_id/users_id/td-initial-values
    location: null
    table_pos: -1                                                      # position in the table array (always < 0)
    version_pos: 0                                                     # position in the versions array
  input:                                                               # array with all input tables
  - !Table
    name: emails                                                      
    table: td://eu/users/emails@HEAD                                  
    table_id: td://eu_id/users_id/emails@v_id                          
    location: null                                                     
    table_pos: 0                                                       # position in the table array (always >= 0)
    version_pos: 0                                                     # position in the versions array
  system_output:                                                       # array with all system output tables
  - !Table
    name: td-initial-values
    location:                                                          
      uri: file:///../.tdserver/default/s/eu_id/d/users_id/v/v_id/t/td-initial-values.t   # URI to the table data (file://, s3://, azure://, gcp://)
      env_prefix: null                                                                    # Prefix env vars with cloud configs/credentials to store the table data
    table_pos: -1                                                      # position in the table array (always < 0)
  output:                                                              # array with all output tables
  - !Table
    name: users                                                        # table name
    location:
      uri: file:///users/tucu/.tdserver/default/s/ID1/d/ID2/v/ID3/ID4/t/users.t # URI to the table data (file://, s3://, azure://, gcp://)
      env_prefix: null                                                          # Prefix env vars with cloud configs/credentials to store the table data
    table_pos: 0                                                       # position in the table array (always >= 0)
```

### response.yaml

```
id: id
class: ephemeral
worker: dataset
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
    name: td-initial-values    # table name
  output:
  - !NoData                    # indicates that the table was not written with new data as part of the function run
    name: users
```

## Dependency run

Same comments as before, new comments show additional information specific to the dependency run.

### request.yaml

```
...
!V1
info:
  dataset: td:///eu/users                                            
  dataset_id: td:///eu_id/users_id                                   
  function_id: f_id                                                  
  function_bundle:
    uri: file:///../.tdserver/default/s/eu_id/d/users_id/f/f_id.f    
    env_prefix: null                                                 
  dataset_data_version: v_id                                         
  execution_plan_id: ep_id                                           
  execution_plan_dataset: td://eu/users                              
  execution_plan_dataset_id: td:///eu_id/users_id   
system_input:                                                        
- !Table
  name: td-initial-values                                           
  table: td:///eu/users/td-initial-values@HEAD~1
  table_id: td:///eu_id/users_id/td-initial-values
  location: null
  table_pos: -1                                                      
  version_pos: 0                                                     
input:                                                               
- !TableVersions                                       # the dependency refers to multiple versions, a dataframe per each will be
  - name: emails                                                      
    table: td://eu/users/emails@HEAD,HEAD^1                                  
    table_id: td://eu_id/users_id/emails@v_id                          
    location: 
      uri: file:///../.tdserver/default/s/eu_id/d/users_id/v/v_id/t/emails.t   
      env_prefix: null                                                
    table_pos: 0                                                       
    version_pos: 0
  - name: emails                                                      
    table: td://eu/users/emails@HEAD,HEAD^1                                  
    table_id: td://eu_id/users_id/emails                          
    location: null                                             
    table_pos: 0                                                       
    version_pos: 1
- !Table              
  - name: last_login                                                      
    table: td://eu/users/last_login@HEAD^^                                  
    table_id: td://eu_id/last_login_id/emails       # this version does not exist                   
    location: null                                  # a NULL data frame will be provided to the function                                 
    table_pos: 1                                                       
    version_pos: 0                                          
system_output:                                                       
- !Table
  name: td-initial-values
  location:                                                          
    uri: file:///../.tdserver/default/s/eu_id/d/users_id/v/v_id/t/td-initial-values.t   
    env_prefix: null                                                                    
  table_pos: -1                                                      
output:                                                              
- !Table
  name: users                                                        
  location:
    uri: file:///../.tdserver/default/s/ID1/d/ID2/v/ID3/ID4/t/users.t 
    env_prefix: null                                                          
  table_pos: 0   
```

### response.yaml

```
...
!V1
system_output:
- !Data                      
  name: td-initial-values    
output:
- !Data
  name: users
```

## Dataset Function Worker logic before running the dataset function

For each table in the `request.yaml` if the URI location is not null, create a lazy frame with it.
For now only file:// URIs are supported (thus we can ignore the env_prefix field).

For tables that are multiple versions (TableVersions), versions are ordered by table position, and then version
position.
Version position refers to the relative version after the version has been resolved (i.e. list into multiple versions).

Map the lazy frames and lazy frame arrays to the corresponding function parameters.

If the function uses initial values (importer), use the lazy frame for the `td-initial-values.t` to extract the initial
values for dataset function, if not present there, extract them from the function decorator. Pass the initial values
to the importer logic.

When the function ends, write all returned dataframes to the corresponding output URIs and set the `response.yaml`
accordingly with !Data(table_name). Once we have the wrapper table frame API, and we can detect that the input
table frame has not been modified, we can set the `response.yaml` with !NoData(table_name).

Also, we will deal with partitioned-tables once we have the table frame API.

Any new initial values are to be written to the provided output `td-initial-values.t` URI to be picked up by the next
run.