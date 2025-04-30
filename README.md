# netlolca
A Python package with tools for interacting with openLCA (version 2) and for developing and reporting NETL LCA unit processes.

The foundation of all tools in this package is the NetlOlca class.
It is designed to provide a single set of methods for querying and editing openLCA databases either directly with openLCA app or indirectly via exported JSON-LD zip files.

Connection to a database is a two-step process:

1. Use `open` to create a file handle with a JSON-LD zip file.

    OR use `connect` to establish an IPC-sever connection using a designated (or default) port.
2. Use `read` to query all root entities in the database and store their universally unique identifiers into memory.

Examples of connecting to openLCA databases are shown in the following.

**Opening JSON-LD**

```py
>>> from netlolca.NetlOlca import NetlOlca
>>> netl = NetlOlca()
>>> netl.open("scratch/data/ELCI_1_20191115_091804.zip")
>>> netl.read()
>>> netl.print_unit_groups()
```

**Connecting to IPC Server**

Note that openLCA software must be opened and the IPC developer tool needs to be activated.

```py
>>> from netlolca.NetlOlca import NetlOlca
>>> netl = NetlOlca()
>>> netl.connect()
INFO:netlolca.NetlOlca:connect:Connected on http://localhost:8080
>>> netl.read()
>>> netl.print_providers()
```

The root entities are defined by GreenDelta's [olca-schema](https://github.com/GreenDelta/olca-schema).
A list of entities and their descriptions are provided in the table below.

| Root Entity Name | Description |
| ---------------- | ----------- |
| Actor | A person or organization |
| Currency | Costing currency |
| DQ System | Data quality system, a matrix of quality indicators |
| EPD | Environmental Product System |
| Flow | Everything that can be an input/output of a process |
| Flow property | Quantity used to express amounts of flow |
| Impact category | Life cycle impact assessment category |
| Impact method | An impact assessment method |
| Location | A location (e.g., country, state, or city) |
| Parameter | Input or dependent global/process/impact parameter |
| Process | Systematic organization or series of actions |
| Product system | A product's supply chain (functional unit) |
| Project | An openLCA project |
| Result | A calculation result of a product system |
| Social indicator | An indicator for Social LCA |
| Source | A literature reference |
| Unit group | Group of units that can be inter-converted |

All queries and edits with openLCA databases are through these root entities.
Below is a short-list of NetlOlca methods that work for both database types.

| Method Name | Description |
| :---------- | :---------- |
| `get_actors` | Return a metadata dictionary of actors |
| `get_descriptors` | Return a list of Ref objects for a given entity type |
| `get_exchange_flows` | Return a list of all flow universally unique identifiers |
| `get_flows` | Return a dictionary of input and/or output flow data for a give process |
| `get_input_flows` | Return a dictionary of input exchange flow data for a given process |
| `get_output_flows` | Return a dictionary of out exchange flow data for a given process |
| `flow_is_tracked` | Return true for a product flow |
| `get_num_inputs` | Return a count of input flows for a given process |
| `get_number_product_systems` | Return the count of product systems in a database |
| `get_process_doc` | Return a process's documentation text |
| `get_process_id` | Return the universally unique identifier for a given product system's reference process |
| `get_reviewer` | Return reviewer's name and ID for a given process |
| `get_reference_category` | Return the category name for a product system's reference process |
| `get_reference_description` | Return the description text for a product system's reference process |
| `get_reference_doc` | Return the documentation text for a product system's reference process |
| `find_reference_exchange` | Return an indexed flow exchange for a given process |
| `get_electricity_gen_processes` | Return a list of electricity generation processes and their IDs |
| `match_process_names` | Return a list of process names and IDs for a given pattern |
| `get_reference_flow` | Return the quantitative reference flow for a given product system |
| `get_reference_name` | Return the name of a product system's reference process |
| `get_reference_process` | Return a list of reference processes for a given product system |
| `get_reference_process_id` | Return a list of UUIDs of reference processes for a given product system |
| `get_spec_class` | Return a root entity class of a given name |
| `get_spec_ids` | Return a list of UUIDs associated with a given root entity |
| `print_descriptors` | Print a data property for a given root entity |
| `print_unit_groups` | Print a list of unit group names. |
| `query` | Return the object for a given root entity and UUID |


# Developer's Corner
For developers wanting to test the source code and expand the list of features, the following sections provide instructions for creating a virtual environment with the necessary dependencies.


## Repository Organization

    netlolca/
    ├── demo/           <- demo notebooks
    │   ├── netlolca_demo.ipynb
    │   └── olca_demo.ipynb
    │
    ├── netlolca/       <- Source code for this package.
    │   ├── __init__.py               <- Makes this a Python package.
    │   └── NetlOlca.py               <- Main data handler class.
    │
    ├── .gitignore        <- Git repo ignore list
    ├── DISCLAIMER        <- Government work disclaimer
    ├── LICENSE           <- Package licensing information; CC0 1.0
    │                        https://creativecommons.org/publicdomain/zero/1.0/
    ├── README.md         <- The top-level README.
    └── setup.py          <- Makes package pip installable (`pip install -e .`)
                             (see Installation section for troubleshooting)


## Docstring Style
This package utilizes the [Numpy Style Guide](https://numpydoc.readthedocs.io/en/latest/format.html) for its docstring convention.


## Dependencies

_Standard packages_:

* json
* logging
* os
* re
* shutil
* sys

_Third-party packages_:

* [olca-ipc](https://pypi.org/project/olca-ipc/)
* [pandas](https://pypi.org/project/pandas/)
* [pyyaml](https://pyyaml.org/)


## Virtual Environment Setup
Note that package versions may be different.

Python 3.12

1. `pip install olca-ipc`
2. `pip install pandas`
3. `pip install pyyaml`
4. `pip install jupyterlab` (optional for running demos)
4. `pip install requests` (optional for running demos)


# References

**olca-ipc**

- https://github.com/GreenDelta/olca-ipc.py
- https://greendelta.github.io/openLCA-ApiDoc/ipc/

**olca-schema**

- https://github.com/GreenDelta/olca-schema
- https://greendelta.github.io/olca-schema/
