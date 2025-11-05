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
context: !V2
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
    scheduled_on: {int}                                                # when the yaml was created
  system_input:                                                        # array with all system input tables
  - !Table
    name: td-initial-values           
    collection_id: {c_id}                                            
    collection: {c}                                      
    table_id: {t_id}    
    table_version_id: {tv_id}    
    execution_id: {e_id}                                                                                                
    transaction_id: {t_id}
    function_run_id: {fr_id}                                                                                                
    triggered_on: {int}                                                                                             
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
    execution_id: {e_id}                                                                                                
    transaction_id: {t_id}
    function_run_id: {fr_id}                                                                                                
    triggered_on: {int}                                                
    table_data_version_id: {tdv_id}                                                              
    location:                                                          
      uri: file:///../.tdserver/default/c/{c_id}/d/{tdv_id}/t/{t_id}/{tv_id}.t
      env_prefix: TD_2_   
    table_pos: 0                                                       # position in the table array (always >= 0)
    version_pos: 0                                                     # position in the versions array
    input_idx: 0                                                       # global sequential id of input table/version
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

Example:

```
class: ephemeral
worker: function
action: start
arguments: []
get-states:
  - state-type: map
    state-key: mount_options
      - prefixes: PA_
                  PB_
callback: !Http
  url: http://127.0.0.1:2457/function_run/{function_run_id}
  method: POST
  headers: {}
  body: true
context: !V2
info:
  collection_id: 06BCT5V3SHQ770L1HDGVT6T290
  collection: cn
  function_version_id: 06BCT5V3SLON37VQOHILLN5R38
  function: fn
  function_run_id: 06BCT5V3SLON37VQOHPNPUGNAG
  function_bundle:
    uri: file:///foo6
    env_prefix: PD_
  triggered_on: 0
  transaction_id: 06BCT5V3SLON37VQOI2E5CC2AG
  execution_id: 06BCT5V3SLON37VQOICPFQU7GK
  execution_name: en
  function_data:
    uri: file:///foo0
    env_prefix: null
  scheduled_on: 0
system_input:
- !Table
  name: fn_state_06BCT5V3S5SG72253ODG7G5354
  collection_id: 06BCT5V3SDPR1FAUEHAUNHRSCC
  collection: collection_1
  table_id: 06BCT5V3SDPR1FAUEHHFFA6NG4
  table_version_id: 06BCT5V3SDPR1FAUEHTRTED2EG
  transaction_id: 06BCT5V3SLON37VQOI2E5BB3BH
  execution_id: 06BCT5V3SLON37VQOICPFQV8HJ
  function_run_id: 06BCT5V3SLON37VQOHPNPUGNAG
  triggered_on: 0
  table_data_version_id: 06BCT5V3SDPR1FAUEI3HHVBFD8
  location:
    uri: file:///foo1
    env_prefix: null
  input_idx: 0
  table_pos: -1
  version_pos: 0
input:
- !Table
  name: table_1
  collection_id: 06BCT5V3SHQ770L1H7FNLB5FJO
  collection: collection_1
  table_id: 06BCT5V3SHQ770L1H7KB8BL1J4
  table_version_id: 06BCT5V3SHQ770L1H7VET6KARG
  transaction_id: 06BCT5V3SLON37VQOI2E5BB3BI
  execution_id: 06BCT5V3SLON37VQOICPFQV8HK
  function_run_id: 06BCT5V3SLON37VQOHPNPUGNAG
  triggered_on: 0
  table_data_version_id: 06BCT5V3SHQ770L1H84D745MV0
  location:
    uri: file:///foo2
    env_prefix: PA_
  input_idx: 0
  table_pos: 0
  version_pos: 0
- !Table
  name: table_2
  collection_id: 06BCT5V3SHQ770L1H8AGIBKBIG
  collection: collection_1
  table_id: 06BCT5V3SHQ770L1H8KNNERS0C
  table_version_id: 06BCT5V3SHQ770L1H8OI46J348
  transaction_id: 06BCT5V3SLON37VQOI2E5BB3BH
  execution_id: 06BCT5V3SLON37VQOICPFQV8HJ
  function_run_id: 06BCT5V3SLON37VQOHPNPUGNAG
  triggered_on: 0
  table_data_version_id: 06BCT5V3SHQ770L1H92UUNHHG8
  location:
    uri: file:///foo3
    env_prefix: PA_
  input_idx: 1
  table_pos: 0
  version_pos: 1
- !Table
  name: table_3
  collection_id: 06BCT5V3SHQ770L1H9DIM1KELO
  collection: collection_1
  table_id: 06BCT5V3SHQ770L1H9JJ157F9C
  table_version_id: 06BCT5V3SHQ770L1H9TFJTHH7G
  transaction_id: 06BCT5V3SLON37VQOI2E5BB3BH
  execution_id: 06BCT5V3SLON37VQOICPFQV8HJ
  function_run_id: 06BCT5V3SLON37VQOHPNPUGNAG
  triggered_on: 0
  table_data_version_id: 06BCT5V3SHQ770L1HA6HO00AIC
  location:
    uri: file:///foo4
    env_prefix: PB_
  input_idx: 2
  table_pos: 1
  version_pos: 0
system_output:
- !Table
  name: fn_state_06BCT5V3SHQ770L1HADLBTT360
  collection_id: 06BCT5V3SHQ770L1HAGFSPT2H4
  collection: collection_1
  table_id: 06BCT5V3SHQ770L1HAQSDLBCJ4
  table_version_id: 06BCT5V3SHQ770L1HB7591N1NK
  table_data_version_id: 06BCT5V3SHQ770L1HBCQH917F4
  location:
    uri: file:///foo5
    env_prefix: PC_
  table_pos: -1
output:
- !Table
  name: table_4
  collection_id: 06BCT5V3SHQ770L1HBMNE1NJGC
  collection: collection_1
  table_id: 06BCT5V3SHQ770L1HBT98ABQ00
  table_version_id: 06BCT5V3SHQ770L1HC7TEOGMJ8
  table_data_version_id: 06BCT5V3SHQ770L1HCENCJSGCC
  location:
    uri: file:///foo5
    env_prefix: PC_
  table_pos: 1
- !Table
  name: table_5
  collection_id: 06BCT5V3SHQ770L1HCN6MAUF20
  collection: collection_1
  table_id: 06BCT5V3SHQ770L1HCO9PKDC38
  table_version_id: 06BCT5V3SHQ770L1HD00SIRE9S
  table_data_version_id: 06BCT5V3SHQ770L1HD90NASCB8
  location:
    uri: file:///foo5
    env_prefix: PC_
  table_pos: 1
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
context: !V2
  output:
  - !Data                     # indicates that the table was written with new data as part of the function run
    table: users              # table name  
  - !NoData                   # indicates that the table was not written with new data as part of the function run
    table: users              # table name
  - !Partitions               
    table: users              
    partitions:
        p0: file...
        p1: file...    
```

### Example:

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
context: !V2
    output:
    - !Data
      table: table_1
    - !NoData
      table: table_2
```