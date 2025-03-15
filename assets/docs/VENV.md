<!--
Copyright 2025 Tabs Data Inc.
-->

![TabsData](/assets/images/tabsdata.png)

# Python Environments

## Classification

Tabsdata relies heavily on Python virtual environments to isolate it from the system Python and to offer tailored
environments for each instance and for each group of functions sharing the same requirements. All the environments below
are expected to have installed the Python package **tabsdata** and its dependencies. In some cases they might contain 
additional packages. Some of them can have different versions of Python aligned with the compatibility matrix; others
will support only a specific version of Python.

There are two basic types of environments:

- **Client**: These are the environments where Tabsdata is installed to support general interaction with the system.
  
    There are two kinds of client environments:

  - **Development**: These are environments installed normally in the client side, and will be used to develop and test
    functions and to interact with the Tabsdata servers. Users can be  expected to have some of them, as each 
    environment will reflect the dependencies of sets of functions.

  - **Administration**: These are environments installed normally in the server side. They are used to manage the 
    Tabsdata servers instances. Users are expected to have only one of them per server, although many of them can exist
    if several versions of Tabsdata coexist in the same server. 
 

- **Server**: These are environments installed in the server side to support the execution of functions. 
    There are two kinds of server environments:

  - **Base**: Each Tabsdata instance owns a dedicated virtual environment automatically managed by the Tabsdata system.
    The main purpose of these environments is supporting preparation tasks that require a Python interpreter before
    launching a function execution or other processes that will run on top of a dedicated virtual environment.

  - **Work**: When a function is executed on the server, there is an algorith that decides the configuration of the 
    environment where the function will run. This environment is called the work environment. Work environments 
    specification (Python version and packages dependencies, mainly) are based on the registration context, using the
    specifications of the development environment from where registration is launched. This default behavior can be
    overridden providing a file descriptor that specifies the characteristics of the work environment at runtime. 

This is a conceptual classification, not a strict physical one. In many cases, some of these environments will be just
the same. This is typical, for example, of a first contact with Tabsdata, where a single environment is enough for all
purposes. The only environments that are out of the direct control of the end-user are the server ones, that are managed
by the Tabsdata system. As a consequence of this, server environments can be deleted at any moment (ensuring the server 
is down), as the system is able to recreate them when needed.

## Location

Client virtual environments can be located anywhere in the file system. The only requirements are using a supported
Python version and having the **tabsdata** package installed.

Server virtual environments are located in the **.tabsdata/instances** directory, under the users' home folder.

- Base server environments follow the naming convention td_.<instance>_<tabsdata_version>_<hash>
- Work server environments follow the naming convention td_<hash>

The hash is essentially an attribute that allows deciding whether a function needs to crete a new dedicated work 
environment, or can reuse an existing one. The hash is calculated based on the environment specification, and packages
(local and remote).

Using the Tabsdata version as part of the base server environment allows smoothly supporting multiple versions of
Tabsdata in the same server. Note also that the hash calculated for the base server environments is added a hashed
predefined salt, to avoid collisions with the hash calculated for the work server environments. Therefore, if base and 
some work environments has the same specification, they will have different hashes and, therefore, resolve to different
virtual environments.

## Base Server Environment

The base server environment is completely handled by the system. It's Python interpreter version is predefined, and 
depends on the corresponding Tabsdata version. When a server process is launched from a given client environment, this 
already determines the Tabsdata version in use, and, consequently, all the remaining characteristics of the base server.
Therefore, as opposed to the work server environment, the base server environment is not configurable by the user, and, 
thus, it does not inherit any package present in the client environment used to interact with the server processes.

The base server environment is created with the predefined Python interpreter version that is specified for each 
Tabsdata version. It contains only core Python packages (like pip) and the **tabsdata** package and its dependencies.
Users with access to this environment could potentially install additional packages on it, but this is not recommended, 
it is strongly discouraged, and it is not supported, as this could lead to unexpected behaviors.