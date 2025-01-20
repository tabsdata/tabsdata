<!--
    Copyright 2025 Tabs Data Inc.
-->

# Tabsdata Getting Started Example

## Setting Up the Environment to Run the Example

This example assumes that:

* You are in a command shell within the `examples` directory
 created by the `td examples` command.  
* `Tabsdata` server is installed locally in the current 
   Python environment. 

## Stop `Tabsdata` Server and Check Its Status
```
tdserver stop
tdserver status
```

## Set Up the Example Directory in an Environment Variable

**NOTE:** This is required for the Publisher and Subscriber to determine the 
input and output directories (It has been done this way to make the example
simpler).

### For Linux/OSX
```
export TDX=`pwd`
```

### For Windows
```
set TDX=%cd%
```

## Start `Tabsdata` Server and Check Its Status
```
tdserver start
tdserver status
```

## Login
```
td login localhost --user admin --password tabsdata
```

## Create a Collection
```
td collection create examples --description "Examples"
```

## Create and Test the Publisher

### Register the Publisher
```
td fn register --collection examples --fn-path publisher.py::pub
```

### Trigger the Publisher
```
td fn trigger --collection examples --name pub
```

### Check the Publisher Execution
```
td exec list-trxs
```

### Show Schema of Table populated by the Publisher
```
td table schema --collection examples --name persons
```

## Create and Test the Transformer

### Register the Transformer
```
td fn register --collection examples --fn-path transformer.py::tfr
```

### Trigger the Transformer
```
td fn trigger --collection examples --name tfr
```
### Check the Transformer Execution
```
td exec list-trxs
```

### Show Schema of a Table populated by the Transformer
```
td table schema --collection examples --name spanish
```

## Create and Test the Subscriber

### Register the Subscriber
```
td fn register --collection examples --fn-path subscriber.py::sub
```

### Trigger the Subscriber
```
td fn trigger --collection examples --name sub
```
### Check the Subscriber Execution
```
td exec list-trxs
```

### See the Files Exported by the Subscriber

#### For Linux/OSX
```
ls output/*
```

#### For Windows
```
dir output\*
```

## Trigger the Execution of the Publisher, Transformer and Subscriber

### Delete the Output Files

#### For Linux/OSX
```
rm output/*
```

#### For Windows
```
del output\*
```

### Trigger the Publisher

With the Publisher, Transformer and Subscriber registered, triggering 
the Publisher will trigger the execution of the 3 in order 
(the whole trigger graph).

```
td fn trigger --collection examples --name pub
```

### Check the Transaction Execution
```
td exec list-trxs
```

### See the Files Exported by the Subscriber

#### For Linux/OSX
```
ls output/*
```

#### For Windows
```
dir output\*
```