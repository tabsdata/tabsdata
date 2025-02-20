<!--
Copyright 2025 Tabs Data Inc.
-->

![TabsData](/assets/images/tabsdata.png)

# Tabsdata Settings

## Settings Specification

The global behaviour of a Tabsdata instance can be customized using instance settings. Settings are configurations that apply to any 
worker in the system, starting from the Supervisor.

In order to specify instance settings, there are four levels according to precedence.

Note 1: in ethe explanation below, **INSTANCE** is the name of a Tabsdata instance, which is the basename of the instance path. In standard 
setups, this is the name of any folder immediately under **~/.tabsdata/instances/**.

Note 2. in this document, when some keyword is lower-case, it is expected to exist in all lower-case, and correspondingly if it is upper-case.

* **TD_\<KEY\> Environment Variable**: if an environment variable like this exists, setting **KEY** will be resolved with this value. 

* **TD__\<INSTANCE\>_\<KEY\> Environment Variable**: if an environment variable like this exists, setting **KEY** will be resolved with this value
when setting **KEY** is requested from a worker in instance **INSTANCE**.

* **Entry \<key\>** in file **settings.yaml** in an instance folder: if such a file exists, and has a value for **KEY**, setting **KEY** will be resolved 
with this value when setting **KEY** is requested from a worker of this instance.

* **Entry \<key\>** in default bundled file **settings.yaml**: Tabsdata comes with a default settings specification. If none of the above holds,
setting **KEY** will be resolved with this value from the bundled **settings.yaml**.


If a file named **settings_\<instance\>.yaml** is found in **~/.tabsdata** folder, it will be moved to the instance folder when first loading settings, 
renaming it to the expected file name **settings.yaml**. Therefore, this will happen each time the supervisor is started.

Settings support total hot reload. Thus, any value coming from the first three sources above can be modified to force any instance to use a new value.

## Creating Settings

To create a file **settings.yaml** suited for a given instance, Tabsdata provides the subcommand **settings** for the command **tdserver**.

The command

```tdserver settings```

will create a file **settings.yaml** populated with all the supported settings and their defaults values as specified in the bundled **settings.yaml**.

This is the preferred way to create a base **settings.yaml** to further customize any Tabsdata instance.

Please check the inline help for advanced uses of subcommand **settings**.