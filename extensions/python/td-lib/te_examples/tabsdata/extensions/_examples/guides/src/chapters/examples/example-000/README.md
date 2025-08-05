<!--
    Copyright 2025 Tabs Data Inc.
-->

<p style="text-align:center; padding-right: 6px;">
    <img src="/resources/images/tabsdata.png" alt="Tabsdata" width="380">
</p>

# Walkthrough Example

## Setting Up the Environment to Run the Example

Before beginning this tutorial, open your terminal in the *`1-walkthrough`* subfolder located within the **examples** 
directory you generated using the `td examples` command.

Make sure you are working within an active Python environment, either virtual or system-level, that has the *`tabsdata`* 
package installed.

For this example to work out of the box, both the *`tdserver`* and *`td`* commands must be run on the same host.

## Stop the *Tabsdata Server* and Check Its Status

> **Note:** This helps to ensure that you run the tutorial in a fully functioning tabsdata server.

```
tdserver stop
tdserver status
```

## Start the *Tabsdata Server* and Check Its Status
```
tdserver start
tdserver status
```

## Log In
```
td login --server localhost --user admin --password tabsdata --role sys_admin
```

## Create a *Collection*
```
td collection create --name examples --description "Examples"
```

> **Note:** You may want to use a different name for your collection, especially if you run this tutorial more than 
once.

## Create and Test the *Publisher*

### Register the Publisher
```
td fn register --coll examples --path publisher.py::pub
```

### Trigger the Publisher
```
td fn trigger --coll examples --name pub
```

### Show the Schema of the *Table* Populated by the Publisher
```
td table schema --coll examples --name persons
```

## Create and Test the *Transformer*

### Register the Transformer
```
td fn register --coll examples --path transformer.py::tfr
```

### Trigger the Transformer
```
td fn trigger --coll examples --name tfr
```

### Show the Schema of a *Table* Populated by the Transformer
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

### Inspect the Files Exported by the *Subscriber*

#### For Linux/macOS
```
ls output/*
```

#### For Windows
```
dir output\*
```

## Trigger the Execution of the *Publisher*, *Transformer* and *Subscriber*

### Delete the Output Files

#### For Linux/macOS
```
rm output/*
```

#### For Windows
```
del output\*
```

### Trigger the *Publisher*

Once the *Publisher*, *Transformer* and *Subscriber* are registered, triggering 
the *Publisher* will automatically trigger the other two in sequence (i.e., 
execute the entire trigger graph).

```
td fn trigger --coll examples --name pub
```

### Inspect the Files Exported by the *Subscriber*

#### For Linux/macOS
```
ls output/*
```

#### For Windows
```
dir output\*
```