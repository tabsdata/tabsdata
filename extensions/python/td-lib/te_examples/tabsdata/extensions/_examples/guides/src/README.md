<!--
Copyright 2025 Tabs Data Inc.
-->

<p style="text-align:center; padding-right: 6px;"><img src="/resources/images/tabsdata.png" alt="Tabsdata" width="380"></p>

# TabsData Examples Guide

Welcome to the TabsData Examples Guide!

This guide provides an overview of how to explore and run the TabsData examples that come bundled 
when you install tabsdata on your local machine.

These examples are designed to help you get started quickly and understand how to work with 
publishers, subscribers, and transformers across a variety of input/output backends. Some examples 
also demonstrate more advanced setups, such as chained triggers and multi-stage data flows.

## How to Generate the Examples

You can generate a local copy of the examples suite by running the following command:

```bash
td examples --dir=<path>
```

This will create a new directory at the location you specify, populated with all the available 
example use cases. The folder must not already exist as it will be created by the CLI.

If you prefer a more explicit variant, you can also use the subcommand form:

```bash
td examples generate --dir=<path>
```

Here, *`<path>`* should be replaced with the target directory where you want the examples to be 
created.

## How to Open This Guide

To automatically open this guide in your browser after generating the examples, simply append the 
*`--guide`* flag:

```bash
td examples --dir=<path> --guide
```

This is particularly useful if you are generating the examples for the first time and want to 
immediately start exploring them with the help of this documentation.

You can also open this guide at any time later by running:

```bash
td examples guide
```