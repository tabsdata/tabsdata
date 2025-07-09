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

**NOTE:** It is required that `tdserver` and `td` commands 
are run in the same host to make this example work out of the box.

## Start `Tabsdata` Server and Check Its Status
```
tdserver start
tdserver status
```

## Login
```
td login --server localhost --user admin --password tabsdata --role sys_admin
```

## Create a Collection
```
td collection create --name examples --description "Examples"
```

## Create and Test the Publisher

### Register the Publisher
```
td fn register --coll examples --path publisher.py::pub
```

### Trigger the Publisher
```
td fn trigger --coll examples --name pub
```

### Show Schema of Table populated by the Publisher
```
td table schema --coll examples --name persons
```

## Create and Test the Transformer

### Register the Transformer
```
td fn register --coll examples --path transformer.py::tfr
```

### Trigger the Transformer
```
td fn trigger --coll examples --name tfr
```

### Show Schema of a Table populated by the Transformer
```
td table schema --coll examples --name spanish
```

## Create and Test the Subscriber

### Register the Subscriber
```
td fn register --coll examples --path subscriber.py::sub
```

### Trigger the Subscriber
```
td fn trigger --coll examples --name sub
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
td fn trigger --coll examples --name pub
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