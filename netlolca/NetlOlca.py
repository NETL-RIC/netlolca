#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# NetlOlca.py
#
##############################################################################
# REQUIRED MODULES
##############################################################################
import json
import logging
import os
import re
import shutil
import sys

import yaml
import olca_ipc as ipc
import olca_ipc.rest as rest
import olca_schema as o
import olca_schema.zipio as zio


##############################################################################
# MODULE DOCUMENTATION
##############################################################################
__doc__ = """This module provides the class and function definitions for
interacting with GreenDelta's openLCA (version 2) either directly (via the
IPC server) or indirectly (via an exported JSON-LD zip file).

Last Edited:
    2025-01-21

Examples
--------
Test making actor YAML template.

>>> make_actor_yaml()

Initialize the class.

>>> netl = NetlOlca()

Test that the IPC works (requires that openLCA app is open, a database is
active, and that the IPC service is running).

>>> netl.connect()
>>> netl.read()
>>> netl.print_providers()
>>> ref_p = netl.get_reference_process(as_ref=False)
>>> netl.print_as_dict(ref_p)

Test that JSON-LD connection works.
First, define the file path to the JSON-LD database.
Then, read the process documentation (from the default product system).

>>> home_dir = os.path.expanduser("~")
>>> my_json = os.path.join(
...     home_dir,
...     "Workspace",
...     "olca",
...     "json-ld",
...     "aluminum_single_system.zip"
... )
>>> netl.open(my_json)
>>> netl.read()
>>> rp_doc = netl.get_process_doc(as_dict=False)
>>> netl.print_as_dict(rp_doc)
>>> netl.close()

Test reading unique unit groups from another example JSON-LD.

>>> my_json = os.path.join(
...     home_dir,
...     "Repositories",
...     "keylogiclca",
...     "netlolca",
...     "scratch",
...     "data",
...     "ELCI_1_20191115_091804.zip"
... )
>>> netl.open(my_json)
>>> netl.read()
>>> netl.print_unit_groups()
>>> netl.close()
"""
__all__ = [
    "FUEL_CATS",
    "NetlOlca",
    "get_as_yaml",
    "get_dict_number",
    "make_actor_yaml",
    "pretty_print_dict",
    "print_progress",
    "read_yaml",
    "writeout",
]


##############################################################################
# GLOBALS
##############################################################################
FUEL_CATS = [
    "ALL",
    "BIOMASS",
    "COAL",
    "GAS",
    "GEOTHERMAL",
    "HYDRO",
    "MIXED",
    "NUCLEAR",
    "OFSL",
    "OIL",
    "OTHF",
    "SOLAR",
    "SOLARTHERMAL",
    "WIND",
]
'''list : Electricity baseline primary fuel categories.'''


##############################################################################
# CLASSES
##############################################################################
class NetlOlca(object):
    """A handler class for working with netlolca package.

    Attributes
    ----------
    _docker : bool
        Whether the IPC service connects on localhost (False) or on
        internal.docker.host (True).
    _filename : str
    _spec_map : dict
        A dictionary of root entity data specifications, including name,
        display name, description (info), olca_schema class, and UUID list.
    client : olca_ipc.ipc.Client
        The openLCA software data handler object.
    file : olca_schema.zipio.ZipReader
        The file data handler object.
    logger : logging.Logger
        The Python logging statement object.

    Examples
    --------
    >>> netl = NetlOlca()
    >>> ps_file = "data/aluminum_single_system.zip"
    >>> netl.open(ps_file)
    >>> netl.read()
    >>> netl.get_input_flows()

    Notes
    -----
    Todo:
        -   Consider including third-party package, appdirs, and create a
            netlolca directory in the user's data directory, following the
            convention of USEPA's `esupy` data manager.

    Changelog:
        - v3.0.0:
            - Add Docker support.
            - Update fields in get flows.
            - Add parameter searching.
        - v2.3.1:
            - Hotfix escape character.
            - Move overused info log statement over to debug.
        - v2.3.0:
            - New build supply chain method based on default providers
        - v2.2.0:
            - Separate method for creating root entity dictionary
            - New overwrite method for JSON-LD projects
            - New checks for input and output file definitions
        - v2.1.0:
            - Create generalized :func:`match_process_names` method
            - New flow type tracker method in :func:`get_flows`
    """
    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Global Variables
    # ////////////////////////////////////////////////////////////////////////
    CLIENT_PORT = 8080

    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Initialization
    # ////////////////////////////////////////////////////////////////////////
    def __init__(self):
        self._filename = ""
        self._output_file = None
        self._port = None
        self.client = None
        self.file = None
        self.logger = logging.getLogger("NetlOlca")
        self._spec_map = _root_entity_dict()
        self._docker = check_for_docker()
        self._docker_url = "http://host.docker.internal"
        self._fuel_cats = FUEL_CATS


    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Property Definitions
    # ////////////////////////////////////////////////////////////////////////
    @property
    def filename(self):
        """The file name associated with a file connection.

        Returns
        -------
        str
            File name.
        """
        return os.path.basename(self._filename)

    @property
    def fuel_cats(self):
        """Returns list of eLCI primary fuel categories."""
        return self._fuel_cats

    @property
    def out_file(self):
        """The output file for editing JSON-LD projects.

        Note that JSON-LD projects must be a .zip archive.

        Parameters
        ----------
        val : str
            A file path.
            It does not have to exist.
            Writing to file may fail if the folder path does not exist.

        Returns
        -------
        str
            If un-specified by the user, then return the same file that
            was read (i.e., _filename).

        Raises
        ------
        OSError
            If the folder for the output file does not exist and cannot be
            created.
        TypeError
            If parameter is not a string. File paths must be string.
        """
        if self._output_file is None:
            return self._filename
        else:
            return self._output_file

    @out_file.setter
    def out_file(self, val):
        if not isinstance(val, str):
            raise TypeError("Expected file as string, found %s" % type(val))

        # Folder existence check.
        out_dir = os.path.dirname(val)
        if not os.path.isdir(out_dir):
            try:
                os.makedirs(out_dir)
            except:
                raise OSError(
                    "Failed to create folder for out file! %s" % out_dir)
            else:
                self.logger.info("Created output folder, %s" % out_dir)

        # Zip format check.
        bn, ext = os.path.splitext(val)
        if ext != ".zip":
            self.logger.warning(
                "Output file must be a zip archive! "
                "Changing file extension to '.zip'.")
            val = "".join([bn, ".zip"])

        self._output_file = val

    @property
    def port(self):
        """Port number associated with connecting to openLCA IPC server.

        Returns
        -------
        int
            Port number.

        Raises
        ------
        TypeError
            If port number is not an integer.
        """
        if self._port is None:
            return self.CLIENT_PORT
        else:
            return self._port

    @port.setter
    def port(self, val):
        if not isinstance(val, int):
            try:
                int(val)
            except ValueError:
                raise TypeError("Expected integer, found %s" % type(val))
            else:
                val = int(val)
        if self._port is None:
            self.logger.info("Changing default port to %s" % val)
        else:
            self.logger.info(
                "Changing port from %s to %s" % (str(self._port), str(val)))
        self._port = val

    @property
    def schema_class_names(self):
        """A list of olca_schema class names.

        Returns
        -------
        list
            List of olca_schema class object name strings.
        """
        r_list = []
        for i in dir(o):
            if not i.startswith("__") and i[0].isupper():
                r_list.append(i)
        return r_list

    @property
    def spec_classes(self):
        """A list of olca_schema root entity classes.

        Returns
        -------
        list
            List of olca_schema root entity class objects.
        """
        r_list = []
        for v in self._spec_map.values():
            r_list.append(v['class'])
        return r_list

    @spec_classes.setter
    def spec_classes(self, value):
        raise AttributeError("This property cannot be set this way!")

    @property
    def spec_names(self):
        """A list of names associated with root-entity olca_schema classes.

        These are arbitrary names, all lowercase, not special characters.
        See also display names.

        Returns
        -------
        list
            List of class names (str)
        """
        r_list = []
        for v in self._spec_map.values():
            r_list.append(v['name'])
        return r_list

    @property
    def spec_display_names(self):
        """A list of display names associated with root-entity olca_schema
        classes.

        These are arbitrary names for better user communication.
        For searchability, see names.

        Returns
        -------
        list
            List of class display names (str)
        """
        r_list = []
        for v in self._spec_map.values():
            r_list.append(v['display'])
        return r_list

    @property
    def spec_info_tuples(self):
        """Return list of tuples for openLCA schema main entities.

        Returns
        -------
        list
            A list of tuples (length three) containing a key (int), display
            name (str), and description text (str).
        """
        r_list = []
        for k, v in self._spec_map.items():
            r_tup = (k, v['display'], v['info'])
            r_list.append(r_tup)
        return r_list

    # \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # Class Function Definitions
    # ////////////////////////////////////////////////////////////////////////
    def _add_to_spec_ids(self, d_type, id_list):
        """Append a list of UUIDs to class id list.

        An internal process used when reading a project (or exported project
        file), and saving the UUID strings to the specification map (_spec_map)
        attribute.

        Parameters
        ----------
        d_type : olca_schema.schema
            An openLCA schema class object.
        id_list : list
            A list of UUIDs.

        Raises
        ------
        KeyError
            When olca_schema class is not recognized.
        TypeError
            If the ID list is not typed correctly.
        """
        if d_type not in self.spec_classes:
            raise KeyError
        if not isinstance(id_list, list):
            raise TypeError("Expected list, received %s" % type(id_list))
        d_key = get_dict_number(self._spec_map, d_type, 'class')
        if d_key is not None:
            self._spec_map[d_key]['ids'] += id_list
            # Hotfix: remove duplicate keys
            self._spec_map[d_key]['ids'] = list(
                set(self._spec_map[d_key]['ids']))

    def _clear_map(self):
        """Reset all UUID lists in spec map attribute.
        """
        for k in self._spec_map.keys():
            self._spec_map[k]['ids'] = []

    def _get_from_spec_class(self, d_type, d_prop):
        """Return object from _spec_class matching the data set type and
        attribute property.

        Parameters
        ----------
        d_type : olca_schema.schema.*
            A schema class.
        d_prop : str
            Dictionary key found in the sub-dictionary of _spec_class parameter.

        Raises
        ------
        ValueError
            For an unknown olca_scheme.schema class.
        KeyError
            For an unrecorded olca_schema.schema class.

        Returns
        -------
        object
            The value of the key (attribute property) for the given data set
            type.

        Examples
        --------
        >>> import olca_schema as o
        >>> netl = NetlOlca()
        >>> netl.connect()
        >>> netl.read()
        >>> netl.get_spec_class('flowproperty')
        <class 'olca_schema.schema.FlowProperty'>
        >>> netl._get_from_spec_class(o.FlowProperty, 'display')
        'Flow property'
        """
        if d_type not in self.spec_classes:
            raise ValueError("Data set type not found!")
        d_key = get_dict_number(self._spec_map, d_type, "class")
        if d_key is not None:
            return self._spec_map[d_key][d_prop]
        else:
            raise KeyError("Failed to find class %s" % str(d_type))

    def _list_properties(self, d_obj):
        """Print Python object's properties.

        A helper method for developers to understand what is 'under the hood'
        of GreenDelta's olca_schema classes.

        Parameters
        ----------
        d_obj : A Python class object
            A Python class object (e.g., the olca_schema class, o.Actor)
        """
        for i in dir(d_obj):
            if not i.startswith("__"):
                print(i)

    def add(self, o_class):
        """Add an openLCA root entity to an open project or JSON-LD file.

        Parameters
        ----------
        o_class : olca_schema.*
            Root entity class object from olca-schema.

        Returns
        -------
        bool
            True for successful write/edit/update.
        """
        is_success = False

        # Check for root entity (these are the ones we write to file)
        if type(o_class) in self.spec_classes:
            # Source:
            # https://greendelta.github.io/openLCA-ApiDoc/ipc/data/put.html
            if self.client:
                try:
                    ref = self.client.put(o_class)
                except Exception as e:
                    err_msg = "Failed to update openLCA project. "
                    err_msg += "%s" % str(e)
                    self.logger.error(err_msg)
                else:
                    is_success = True
                    # Fingers crossed this doesn't throw an error:
                    self.logger.info(
                        "Updated %s (%s)" % (ref.id, ref.ref_type.name))
            elif self.file:
                # Close current file reader handle.
                self.close()
                # Check to see if the new class object exists in our file.
                if self.get_from_file(type(o_class), o_class.id) is None:
                    # New entity, write new .json to zip.
                    self.logger.info(
                        "Writing new %s to JSON-LD file" %
                        self._get_from_spec_class(type(o_class), 'display')
                    )
                    try:
                        self.write(o_class, self._filename)
                    except Exception as e:
                        # No UUID will throw value error
                        err_msg = "Failed to write schema to JSON-LD file. "
                        err_msg += "%s" % str(e)
                        self.logger.error(err_msg)
                    else:
                        is_success = True
                    finally:
                        self.open(self._filename)
                        self.read()
                else:
                    # Existing entity, overwrite.
                    self.logger.info(
                        "Overwriting existing %s to JSON-LD file, "
                        "this may take a while." %
                        self._get_from_spec_class(type(o_class), 'display')
                    )
                    is_success = _overwrite(self._filename, o_class)
                    self.open(self._filename)
                    self.read()
            else:
                self.logger.warn("No connection!")
        else:
            self.logger.warn("Unrecognized class, %s" % type(o_class))
        return is_success

    def build_supply_chain(self, pid, e_list=[], p_list=[]):
        """Populate the processes and product link lists based on the
        default providers assigned to a given process.

        If the process ID provided is the reference process for a product
        system, then the list of product links and universally unique
        identifiers of processes may be used to for a product system's
        ``processes`` and ``product_links`` attributes (see example).

        Parameters
        ----------
        pid : str
            A universally unique identifier to a process or a product system;
            if a product system ID is received, the reference process is
            used to build the supply chain.
        e_list : list, optional
            A list of ProcessLinks, by default []
        p_list : list, optional
            A list of Process IDs, by default []

        Returns
        -------
        tuple
            A tuple of length two: list of ProcessLinks and a list of
            Process UUIDs.

        Examples
        --------
        >>> netl.connect()  # connect to IPC server
        >>> netl.read()
        >>> pid = netl.get_spec_ids(o.Process)[0] # select ref process
        >>> pl_list, pd_list = netl.build_supply_chain(pid)
        >>> p_ref = netl.query(o.Process, pid).to_ref()
        >>> p_lc = o.LinkingConfig()
        >>> ps_ref = netl.client.create_product_system(p_ref, p_lc)
        >>> ps_obj = netl.query(o.ProductSystem, ps_ref.id)
        >>> ps_obj.processes = [
        ...     netl.query(o.Process, x).to_ref() for x in pd_list]
        >>> ps_obj.process_links = pl_list
        >>> netl.add(ps_obj)
        """
        # Make sure ID is for a process
        pid = self.get_process_id(pid)
        p_obj = self.query(o.Process, pid)

        if p_obj and (pid not in p_list):
            # Add process ID to process list
            p_list.append(pid)

            # Iterate over exchanges:
            for ex in p_obj.exchanges:
                # The supply chain is only for inputs
                if ex.is_input and (ex.default_provider is not None):
                    p_link = o.ProcessLink(
                        exchange=o.ExchangeRef(internal_id=ex.internal_id),
                        flow=ex.flow,
                        process=p_obj.to_ref(),
                        provider=ex.default_provider,
                    )
                    e_list.append(p_link)

                    # Build supply chain of default provider:
                    e_list, p_list = self.build_supply_chain(
                        ex.default_provider.id, e_list, p_list)

        return (e_list, p_list)

    def close(self):
        """Close an open file connection.
        """
        if self.file:
            self.logger.info("Closing file connection.")
            self.file.close()
        self.file = None

    def connect(self, port=None):
        """Establish connection with openLCA via IPC.

        In Docker containers (i.e., self._docker == True) with GDT server
        (https://github.com/GreenDelta/gdt-server), set the client using
        the RestClient in IPC.

        Parameters
        ----------
        port : int, optional
            HTML port number; if none, defaults to CLIENT_PORT.
        """
        if port is None:
            port = self.port
        if self._docker:
            _url = self._docker_url + ":" + str(port)
            self.client = rest.RestClient(_url)
            if self.client:
                self.logger.info("Connected on %s" % self.client.endpoint)
        else:
            self.client = ipc.Client(port)
            if self.client:
                self.logger.info("Connected on %s" % self.client.url)

    def disconnect(self):
        """Close the client connection with IPC.
        """
        self.logger.info("Disconnecting client from server")
        self.client = None

    def find_parameter(self, p_name, as_dict=False):
        """Return a list of parameter objects (or dictionaries) matching a
        given name.

        Parameters
        ----------
        p_name : str
            Parameter name
        as_dict : bool, optional
            Whether return objects should be dictionaries, by default False

        Returns
        -------
        list
            A list of parameter objects (or dictionaries).
            An empty list indicates no parameter was found.

        Raises
        ------
        ValueError
            If the parameter name is not a string object.
        """
        # Remember that parameter names are case insensitive!
        if not isinstance(p_name, str):
            raise ValueError("Parameter names must be a string!")
        p_name = p_name.lower()

        # Initialize return list of potential matches.
        p_matches = []
        for p_obj in self.list_parameters():
            par_name = p_obj.name.lower()
            if par_name == p_name:
                if as_dict:
                    p_matches.append(p_obj.to_dict())
                else:
                    p_matches.append(p_obj)

        if len(p_matches) == 0:
            self.logger.warning("Failed to find parameter, '%s'" % p_name)
        return p_matches

    def find_parameter_process(self, name=None, uuid=None):
        """Return a list of potential process UUIDs that use or reference
        a given parameter UUID or name.

        Parameters
        ----------
        name : str, optional
            Parameter name, by default None
        uuid : str, optional
            Parameter universally unique identifier, by default None

        Returns
        -------
        list
            A list of process UUIDs that match either the parameter
            UUID or the parameter name.

        Raises
        ------
        ValueError
            If no parameter name or UUID is provided.
        TypeError
            If a non-string object is given for a parameter name or
            parameter UUID.
        """
        if not name and not uuid:
            raise ValueError(
                "You must provide at least a process name or UUID")
        if name and not isinstance(name, str):
            raise TypeError("Parameter name must be a string!")
        if uuid and not isinstance(uuid, str):
            raise TypeError("UUID must be a string!")

        proc_list = []
        for pid in self.get_spec_ids(o.Process):
            p_obj = self.query(o.Process, pid)
            for par_obj in p_obj.parameters:
                # Prioritize UUID over parameter name:
                if uuid and par_obj.id == uuid:
                    proc_list.append(pid)
                elif name and par_obj.name.lower() == name.lower():
                    proc_list.append(pid)

        if len(proc_list) == 0:
            self.logger.warning("Parameter not found in a process!")
        return proc_list

    def find_process_parameters(self, uuid):
        """Return parameter objects associated with a process.

        Includes parameters found in the parameters table and referenced
        in the exchange table. Recursively searches dependent/calculated
        parameters to find all process and global parameters either directly
        or indirectly used in a given process. This method is for generating
        a complete parameter reference table for a unit process.

        Parameters
        ----------
        uuid : str
            A Process universally unique identifier.

        Returns
        -------
        list
            A list of Parameter objects.
            Includes both Process and Global scope parameters and both
            input and calculated parameters.

        Raises
        ------
        TypeError
            If the UUID received is not a string object.
        ValueError
            If the UUID received is not for a known process.
        """
        if not isinstance(uuid, str):
            raise TypeError("UUID must be a string!")
        if not uuid in self.get_spec_ids(o.Process):
            try:
                self.logger.info(
                    "Received non-process UUID, trying to get process ID")
                return self.find_process_parameters(self.get_process_id(uuid))
            except:
                raise ValueError("UUID must be for a process!")

        # Initialize the return object
        param_list = []

        # Process-level
        self.logger.info("Searching process-level parameters")
        p_obj = self.query(o.Process, uuid)
        for par_obj in p_obj.parameters:
            param_list.append(par_obj)

            # Get referenced global params in non-input process params
            # Remember, global params can be dependent!
            if not par_obj.is_input_parameter and par_obj.formula:
                # Search the formula for global parameters.
                # NOTE: process-level params are already captured above.
                f_params = self.formula_extractor(par_obj.formula, [])
                param_list += f_params

        # Exchange table referenced (global level)
        self.logger.info("Searching exchange table parameters")
        for ex_obj in p_obj.exchanges:
            # Formula is none unless defined, then it is a string.
            if ex_obj.amount_formula:
                # Again, only get global params, because process-level is
                # accounted for.
                f_params = self.formula_extractor(ex_obj.amount_formula, [])
                param_list += f_params

        if len(param_list) == 0:
            self.logger.warning("No parameters found for process!")
        else:
            # Remove duplicates
            r_list = []
            tmp_list = []
            for param in param_list:
                my_tuple = (param.id, param.parameter_scope.name)
                if my_tuple not in tmp_list:
                    tmp_list.append(my_tuple)
                    r_list.append(param)
            param_list = r_list

        return param_list

    def find_ref_exchange(self, process_uuid, exchange_id):
        """Returns an openLCA Exchange class object matching the exchange ID.

        All exchanges from the given process are queried until the
        exchange ID attribute is matched.

        Parameters
        ----------
        process_uuid : str
            Process universally unique identifier (UUID)
        exchange_id : int
            Exchange ID

        Returns
        -------
        olca_schema.Exchange
            Exchange class object.

        Raises
        ------
        ValueError
            When process ID is not from a known process.
        """
        if not process_uuid in self.get_spec_ids(o.Process):
            raise ValueError("UUID must be a process!")
        p = self.query(o.Process, d_uuid=process_uuid)
        for exchange in p.exchanges:
            if exchange.internal_id == exchange_id:
                return exchange

    def flow_is_tracked(self, uuid):
        """Check a flow ID to see if it is a Product Flow.

        Parameters
        ----------
        uuid : str
            Flow universally unique identifier.

        Returns
        -------
        str
            Yes for product flow or no (for all others).
            Note that non-flow IDs will return 'No'.
        """
        f = self.query(o.Flow, uuid)
        r_str = "No"
        if f is not None:
            if f.flow_type == o.FlowType.PRODUCT_FLOW:
                r_str = "Yes"
        else:
            self.logger.warning("Failed to find flow, '%s'" % uuid)
        return r_str

    def formula_extractor(self, f_str, p_list=[]):
        """Extract global parameters from a formula.

        The process includes the following steps to recursively search
        parameters in a formula to find all input global parameters.

        1.  Read through a given formula,
        2.  Extract parameters from formula (assumes parameter naming scheme)
        3.  Add global parameters to the list
        4.  If global parameter is calculated, search its formula

        Parameters
        ----------
        f_str : str
            A formula (e.g., 'val_1*kcal/const_c').
            Formulas are found in dependent/calculated parameters and within
            exchange tables (e.g., amount formula).
        p_list : list, optional
            A list of parameters used for recursive searching, by default []

        Returns
        -------
        list
            A list of global parameters found in a formula, including
            any dependent global parameters found in a calculated parameter's
            formula.

        Notes
        -----
        The parameter naming scheme is based on the notion that variables
        are letters, numbers, and underscores that does not start or end
        with an underscore.
        """
        # Essentially, 'GLOBAL_SCOPE' str
        g_scope = o.ParameterScope.GLOBAL_SCOPE.name

        # Use regular expression to match variable names
        # (based on the notion that variables are letters,
        # numbers, and underscores that does not start or end
        # with underscore). The group is: starts with one or
        # more word characters ([a-zA-Z_0-9]) followed by any
        # number of: underscore with one or more words after it.
        # Source: RootTwo (https://stackoverflow.com/a/35169937)
        q = re.compile("((?:\\w)+(?:_\\w+)*)")

        # Create a secondary regex to filter out parameters that consist
        # entirely of numbers (i.e., coefficients or constants in equations).
        q_except = re.compile("[0-9]+")

        # Search the formula for parameters & filter out exceptions.
        f_params = re.findall(q, f_str.lower())
        f_params = [
            x for x in f_params if (not re.match(q_except, x)) and (
                x != "if")
        ]

        for f_param in f_params:
            s_results = self.find_parameter(f_param)
            for sr in s_results:
                # Add all referenced global parameters
                if sr.parameter_scope.name == g_scope:
                    p_list.append(sr)
                    # Recursively search dependent global parameters:
                    if not sr.is_input_parameter and sr.formula:
                        p_list = self.formula_extractor(sr.formula, p_list)
        return p_list

    def get_actor_yaml(self, f_dir=None):
        """Helper method to return the file (or file path) to the actor YAML.

        Parameters
        ----------
        f_dir : str, optional
            A directory path, by default None

        Returns
        -------
        str
            The name of the actor YAML file. If a directory name was given,
            it is appended to the file name to provide a file path.
        """
        f_name =  self._get_from_spec_class(o.Actor, 'yaml')
        if f_dir is not None and isinstance(f_dir, str):
            f_name = os.path.join(f_dir, f_name)
        return f_name

    def get_actors(self, unique=False, add_yaml=None):
        """Return dictionary of all actor information from a openLCA project.

        Parameters
        ----------
        unique : bool, optional
            If true, only uniquely named actors are returned.
            Inconsistencies within other attributes (e.g., address,
            telephone, or email) are not considered in the uniqueness
            test. Defaults to false.
        add_yaml : str, optional
            A valid file path to a YAML with actor element entries.
            The expectation is that another program (e.g., Interface in UP
            tempate) defines the YAML file and passes to this method, where
            it can be read and appended to an actor dictionary.
            Defaults to None.

        Returns
        -------
        dict
            A dictionary of actor info for a unit process openLCA project with
            keys 'name', 'description', 'tags', 'address', 'city', 'country',
            'email', 'telefax', 'telephone', 'website', 'zip_code', and 'uuid'
            formatted ready for conversion to a pandas data frame.
        """
        a_list = self.get_all(o.Actor)
        # Append YAML-file actors, if requested to:
        if (add_yaml is not None
                and isinstance(add_yaml, str)
                and os.path.isfile(add_yaml)):
            b_list = self.get_yaml_entities(add_yaml)
            a_list += b_list

        r_dict = {
            'name': [],
            'description': [],
            'tags': [],
            'address': [],
            'city': [],
            'country': [],
            'email': [],
            'telefax': [],
            'telephone': [],
            'website': [],
            'zip_code': [],
            'uuid': [],
        }
        for a in a_list:
            a_name = "%s" % a.name
            a_des = "%s" % a.description
            a_tags = ",".join(a.to_dict().get("tags", []))
            a_addr = "%s" % a.address
            a_city = "%s" % a.city
            a_ctr = "%s" % a.country
            a_email = "%s" % a.email
            a_fax = "%s" % a.telefax
            a_tel = "%s" % a.telephone
            a_web = "%s" % a.website
            a_zip = "%s" % a.zip_code
            a_uid = "%s" % a.id
            # Handle unique case based on duplicate 'name' entries
            if not unique or (unique and a_name not in r_dict['name']):
                r_dict['name'].append(a_name)
                r_dict['description'].append(a_des)
                r_dict['tags'].append(a_tags)
                r_dict['address'].append(a_addr)
                r_dict['city'].append(a_city)
                r_dict['country'].append(a_ctr)
                r_dict['email'].append(a_email)
                r_dict['telefax'].append(a_fax)
                r_dict['telephone'].append(a_tel)
                r_dict['website'].append(a_web)
                r_dict['zip_code'].append(a_zip)
                r_dict['uuid'].append(a_uid)
        return r_dict

    def get_all(self, d_type=o.UnitGroup):
        """Return a list of olca_schema.schema class objects of a given
        root entity type.

        Note that this is different from :func:`get_descriptors`, which
        returns only Ref objects, which tend to be quite smaller than the
        native classes. Be careful when using this on rather large lists
        (e.g., Flows or Processes).

        Parameters
        ----------
        d_type : olca_schema.schema, optional
            An openLCA schema class.
            Defaults to o.UnitGroup.

        Returns
        -------
        list
            A list of olca_schema.schema objects of the same data type
            requested.
        """
        r_list = []
        if d_type not in self.spec_classes:
            self.logger.warning("Data type not supported!")
            return r_list

        if self.client:
            try:
                r_list = self.client.get_all(d_type)
            except ConnectionRefusedError as e:
                self.logger.warning(
                    "Server may have stopped! "
                    "Please check connection.\n%s" % e)
            else:
                self.logger.debug("Found %d unit groups." % len(r_list))
        elif self.file:
            id_list = self.get_spec_ids(d_type)
            for id_ in id_list:
                ref = self.get_from_file(d_type, id_)
                if ref is not None:
                    r_list.append(ref)
        else:
            self.logger.warning("No connection!")
        return r_list

    def get_allocation_info(self, uuid=None):
        """
        Extract allocation-related information from a Process object.

        This method retrieves allocation-specific attributes from an instance
        of the Process class, packaging the information into a dictionary
        for easy access and further processing.

        Notes
        -----
        1.  The extraction focuses on attributes related to allocation within
            the Process object.
        2.  If the provided object is not an instance of the Process class or
            lacks the expected allocation attributes, the method might not
            behave as expected. Always ensure that a valid Process object is
            provided.

        Parameters
        ----------
        uuid : str
            A universally unique identifier to a product system or its
            reference process.

        Returns
        -------
        tuple
            A tuple containing allocation_factors and default_allocation_method.
            The first element of the tuple is a list of allocation factors, and
            the second element is an olca_schema.AllocationType object (enum)
            describing the default allocation method or NoneTypes.
        """
        self.logger.info("Querying allocation method")
        uuid = self.get_process_id(uuid)
        process_obj = self.query(o.Process, uuid)
        allocation_factors = process_obj.allocation_factors
        default_allocation_method = process_obj.default_allocation_method

        return allocation_factors, default_allocation_method

    def get_consumption_mix(self, uuid):
        """Convenience function to convert consumption mix in eLCI into their
        primary fuel mix by examining each of the providers' generation mixes
        multiplied by the provider-level mix.

        Parameters
        ----------
        uuid : str
            Universally unique identifier for a consumption mix process.

        Returns
        -------
        tuple
            Tuple of length two.

            - dict: Consumption mixes by primary fuel category.
            - list: Providers used in consumption mix.

        Raises
        ------
        ValueError
            If the UUID received is not an electricity consumption mix.
        """
        # Get UUIDs for consumption mixes
        cons = self.get_electricity_con_processes()
        cons_uuids = [x[0] for x in cons]

        # Check that we received a consumption mix process UUID
        if uuid not in cons_uuids:
            raise ValueError("Failed to find consumption mix in dataset!")

        # Initialize fuel mix dictionary and provider list, ba_list.
        self.logger.info("Initializing fuel mix dictionary")
        fuel_dict = dict()
        for f_cat in self.fuel_cats:
            fuel_dict[f_cat] = 0.0

        ba_list = []

        # Initialize fuel name query, as defined in eLCI.
        # See electricitylci/process_dictionary_writer.py#L224 (1073add)
        q_fuel = re.compile("^from (\\w+) - (.*)$")

        # Consumption flows are by area (e.g., BA).
        # Get ba mix values, then search provider for fuel-based inventory
        cons_flows = self.get_flows(uuid, inputs=True, outputs=False)
        ba_mixes = cons_flows['amount']
        ba_uuids = cons_flows['provider']
        ba_names = cons_flows['description']
        num_mixes = len(ba_mixes)
        logging.info("Processing %d BA areas" % num_mixes)
        for i in range(num_mixes):
            # This is the BA mix coefficient.
            ba_mix = ba_mixes[i]
            ba_list.append(ba_names[i])

            # Get input exchange values---these should be for primary fuels
            ba_uid = ba_uuids[i]
            ba_exchanges = self.get_flows(ba_uid, inputs=True, outputs=False)
            ba_fuel_mixes = ba_exchanges['amount']
            ba_fuel_descr = ba_exchanges['description']
            # Pull fuel names from description text.
            ba_fuel_names = []
            for ba_fuel in ba_fuel_descr:
                r = q_fuel.match(ba_fuel)
                f_name = ""
                if r:
                    self.logger.info(
                        "Found %s for %s" % (f_name, ba_names[i]))
                    f_name = r.group(1)
                ba_fuel_names.append(f_name)

            num_fuels = len(ba_fuel_mixes)
            for j in range(num_fuels):
                fuel_name = ba_fuel_names[j]
                fuel_mix = ba_fuel_mixes[j]

                # Here's the math:
                # Update the value of fuel_dict[fuel_name] with
                # `ba_mix` * `fuel_mix`. Because both coefficients
                # are fractions of one, we should be able to just
                # sum them up across all BA areas in the U.S.
                # for each fuel category
                fuel_dict[fuel_name] += ba_mix*fuel_mix

        return (fuel_dict, ba_list)

    def get_descriptors(self, d_type=o.FlowProperty):
        """Return list of data set descriptors for a given data set type.

        Parameters
        ----------
        d_type : olca_schema class (e.g., o.FlowProperty), optional
            The data set type to retrieve descriptors of.
            If no data set type provided, defaults to FlowProperty.

        Returns
        -------
        list
            A list of olca_schema.schema.Ref objects.
            If none, returns an empty list.
        """
        r_list = []
        if not d_type in self.spec_classes:
            self.logger.warning("Unknown data set type!")
            return r_list

        if self.client:
            try:
                r_list = self.client.get_descriptors(d_type)
            except ConnectionRefusedError as e:
                self.logger.warning(
                    "Server may have stopped! "
                    "Please check connection.\n%s" % e)
            else:
                self.logger.debug("Found %d descriptors." % len(r_list))
        elif self.file:
            r_list = self.get_all(d_type)
            r_list = [a.to_ref() for a in r_list]
        else:
            self.logger.warning("No connection!")
        return r_list

    def get_electricity_con_processes(self):
        """Get list of process UUIDs for each consumption mix at grid.

        Returns
        -------
        list
            List of tuples, each tuple of length two (may return empty
            if no electricity generation processes are found).

            - str: process UUID
            - str: Balancing Authority name

        Notes
        -----
        This method assumes the electricity generation processes at grid
        are based on the 2016 electricity baseline (as provided through the
        Federal LCA Commons), where the generation processes for each
        Balancing Authority area are titled as follows:
        'Electricity; at grid; generation mix - BALANCING AUTHORITY NAME',
        where BALANCING AUTHORITY NAME is the title case for a Balancing
        Authority area (e.g., Arizona Public Service Company).
        """
        q = re.compile("^Electricity; at grid; consumption mix - (.*)$")
        r = self.match_process_names(q)
        return r

    def get_electricity_gen_processes(self, residual=False):
        """Get list of process UUIDs for each generation mix at grid.

        Parameters
        ----------
        residual : bool, optional
            Whether residual generation processes should be returned.
            Defaults to false.

        Returns
        -------
        list
            List of tuples, each tuple of length two (may return empty
            if no electricity generation processes are found).

            - str: process UUID
            - str: Balancing Authority name

        Notes
        -----
        This method assumes the electricity generation processes at grid
        are based on the 2016 electricity baseline (as provided through the
        Federal LCA Commons), where the generation processes for each
        Balancing Authority area are titled as follows:
        'Electricity; at grid; generation mix - BALANCING AUTHORITY NAME',
        where BALANCING AUTHORITY NAME is the title case for a Balancing
        Authority area (e.g., Arizona Public Service Company).

        When searching for residual mixes, the process name follows:
        'Electricity; at grid; residual generation mix' followed by the
        balancing authority name.
        """
        q = re.compile("^Electricity; at grid; generation mix - (.*)$")
        if residual:
            q = re.compile(
                "^Electricity; at grid; residual generation mix - (.*)$")
        r = self.match_process_names(q)
        return r

    def get_exchange_flows(self):
        """Return a list of unique flows found in process exchanges.

        This list may be used to compare against the root entity list of flows
        to find any superfluous flow data not represented by any process.

        Notes
        -----
        Flows in Exchanges are found in 'exchanges' lists of each Process;
        see https://greendelta.github.io/olca-schema/classes/Process.html.

        Returns
        -------
        list
            A sorted list of flow universally unique identifiers (UUIDs)
            associated with process exchanges.
        """
        f_ids = []
        # Be memory conscience and load one process at a time.
        for pid in self.get_spec_ids(o.Process):
            p = self.query(o.Process, pid)
            if p:
                for e in p.exchanges:
                    # HOTFIX NoneType exchanges
                    if e.flow:
                        f_ids.append(e.flow.id)
        return sorted(list(set(f_ids)))

    def get_flow_by_exchange(self, uuid, ex_id):
        if not isinstance(ex_id, int):
            raise ValueError(
                "Exchange ID must be an integer! Received %s" % type(ex_id))

        p = self.query(o.Process, uuid)
        if p:
            for ex in p.exchanges:
                if ex.internal_id == ex_id:
                    self.logger.info("Found exchange %d" % ex_id)
                    return ex
        else:
            self.logger.warning("Failed to find Process UUID!")
        self.logger.info("Failed to find exchange ID, %d" % ex_id)
        return None

    def get_flows(self, uuid=None, inputs=True, outputs=True):
        """Return dictionary of flow data associated with a process's exchanges.

        If no UUID is provided (or a UUID of a product system), then the
        reference process is queried.

        Parameters
        ----------
        uuid : str, optional
            Universally unique identifier (UUID), by default None
        inputs : bool, optional
            If input flows are requested, by default true
        outputs : bool, optional
            If output flows are requested, by default true

        Returns
        -------
        dict
            A dictionary with 'name', 'amount', 'unit', 'category', 'uuid',
            'tracked', 'description', 'provider', and 'dq' (data quality)
            fields formatted ready for conversion to a pandas data frame.

        Notes
        -----
        The data quality index is a string (e.g., '(1;3;2;5;1)').
        """
        r_dict = {
            'name': [],
            'amount': [],
            'unit': [],
            'category': [],
            'uuid': [],
            'tracked': [],
            'description': [],
            'provider': [],
            'dq': [],
        }
        uuid = self.get_process_id(uuid)
        p = self.query(o.Process, uuid)
        if p is not None:
            for e_obj in p.exchanges:
                e_tracked = self.flow_is_tracked(e_obj.flow.id)
                e_name = "%s" % e_obj.flow.name
                e_amount = e_obj.amount
                e_unit = "%s" % e_obj.unit.name
                e_cat = "%s" % e_obj.flow.category
                e_uid = "%s" % e_obj.flow.id
                e_des = "%s" % e_obj.description
                e_prov = "%s" % e_obj.to_dict().get(
                    "defaultProvider", {}).get("@id", "")
                e_dq = "%s" % e_obj.to_dict().get('dq_entry', '')
                if inputs and e_obj.is_input:
                    r_dict['name'].append(e_name)
                    r_dict['amount'].append(e_amount)
                    r_dict['unit'].append(e_unit)
                    r_dict['category'].append(e_cat)
                    r_dict['uuid'].append(e_uid)
                    r_dict['tracked'].append(e_tracked)
                    r_dict['description'].append(e_des)
                    r_dict['provider'].append(e_prov)
                    r_dict['dq'].append(e_dq)
                if outputs and not e_obj.is_input:
                    r_dict['name'].append(e_name)
                    r_dict['amount'].append(e_amount)
                    r_dict['unit'].append(e_unit)
                    r_dict['category'].append(e_cat)
                    r_dict['uuid'].append(e_uid)
                    r_dict['tracked'].append(e_tracked)
                    r_dict['description'].append(e_des)
                    r_dict['provider'].append(e_prov)
                    r_dict['dq'].append(e_dq)

        return r_dict

    def get_from_file(self, d_class, d_uuid):
        """Return the schema object for a given UUID of a given data type
        as read from a JSON-LD project file.

        Parameters
        ----------
        d_class : olca_schema.schema object
            A schema class object.
        d_uuid : str
            A universally unique identifier string.

        Raises
        ------
        ValueError
            If schema class is unknown.

        Returns
        -------
        olca_schema.schema object
            An instance of an olca_schema.schema object.
        """
        if d_class not in self.spec_classes:
            raise ValueError("Unknown schema class, %s" % str(d_class))

        r_obj = None
        try:
            r_obj = self.file.read(d_class, d_uuid)
        except Exception as e:
            d_name = self.get_spec_name(d_class)
            self.logger.warning(
                "Failed to read %s (%s) from file! %s" % (
                    d_name, d_uuid, str(e)))

        return r_obj

    def get_input_flows(self, uuid=None):
        """Return dictionary of input exchange flow data for a given process.

        If no UUID is provided (or a UUID of a product system), then the
        reference process is queried.

        Parameters
        ----------
        uuid : str, optional
            Universally unique identifier (UUID), by default None

        Returns
        -------
        dict
            A dictionary with 'name', 'amount', 'unit', 'category', and 'uuid'
            fields formatted ready for conversion to a pandas data frame;
            see example on how.

        Examples
        --------
        >>> import pandas as pd
        >>> netl = NetlOlca()
        >>> netl.connect()
        >>> d = netl.get_input_flows()
        >>> df = pd.DataFrame(data=d)
        >>> df.head()
                                                  name    amount unit ...
        0                         Silica sand, at mine  0.035380   kg ...
        1                          Quicklime, at plant  0.000028   kg ...
        2                       Filter media, at plant  0.000058   kg ...
        3       Treatment gases, unspecified, at plant  0.011000   kg ...
        4  Natural gas, combusted in industrial boiler  0.563500   m3 ...
        """
        return self.get_flows(uuid=uuid, inputs=True, outputs=False)

    def get_num_inputs(self, pid):
        """Return the number of input exchanges for a given process.

        Parameters
        ----------
        pid : str
            Process universally unique identifier (UUID)

        Returns
        -------
        int
            Number of input exchanges
        """
        p_obj = self.query(self.get_spec_class("Process"), pid)
        num_ex = 0
        if p_obj is not None:
            for exc in p_obj.exchanges:
                if exc.is_input:
                    num_ex += 1
        else:
            self.logger.warning("Process '%s' not found!" % pid)
        return num_ex

    def get_number_product_systems(self):
        """Return the number of product systems found in current project.

        Requires a project to be read.

        Returns
        -------
        int
            Number of project systems found.
        """
        d_class = self.get_spec_class("Product system")
        d_ids = self._get_from_spec_class(d_class, 'ids')
        return len(d_ids)

    def get_output_flows(self, uuid=None):
        """Return dictionary of output exchange flow data for a given process.

        If no UUID is provided (or a UUID of a product system), then the
        reference process is queried.

        Parameters
        ----------
        uuid : str, optional
            Universally unique identifier (UUID), by default None

        Returns
        -------
        dict
            A dictionary with 'name', 'amount', 'unit', 'category', and 'uuid'
            fields formatted ready for conversion to a pandas data frame.
        """
        return self.get_flows(uuid=uuid, inputs=False, outputs=True)

    def get_parameter_sets(self, uuid, as_dict=False):
        # IN PROGRESS
        # Return a product system's parameter sets.
        # These represent the adjustable process parameters.

        if not isinstance(uuid, str) or uuid == "":
            raise ValueError("UUID must be a valid string!")
        if uuid not in self.get_spec_ids(o.ProductSystem):
            raise ValueError(
                "UUID not found! Provide a product system UUID.")

        # Initialize return list
        ps_list = []

        # Query for product system
        ps_obj = self.query(o.ProductSystem, uuid)

        # Iterate over each parameter set (e.g., Baseline), and add them to
        # the return list, skipping over sets with no parameter redefinitions.
        for p_set in ps_obj.parameter_sets:
            if p_set.parameters:
                if as_dict:
                    ps_list.append(p_set.to_dict())
                else:
                    ps_list.append(p_set)

        return ps_list

    def get_process_doc(self, uuid=None, as_dict=True):
        """Return a process's documentation.

        If a UUID is provided, the associated process will be queried;
        otherwise, the project's reference process (i.e., the process based
        on the product system) is queried.

        The process documentation is a schema object inherited under a
        Process class. See also:
        https://greendelta.github.io/olca-schema/classes/Process.html

        Parameters
        ----------
        uuid : str, optional
            The UUID string for a given process, by default None
        as_dict : bool, by default True
            Switch for return type.
            If true, return type is a dictionary; otherwise, the return type
            is a ProcessDocumentation class object as defined in olca_scheam.

        Returns
        -------
        dict, or olca_schema.schema.ProcessDocument
            A data class for olca_schema Process objects returned either as
            a class object or as a dictionary (e.g., if ``as_dict`` is true).
        """
        uuid = self.get_process_id(uuid)
        my_p = self.query(o.Process, uuid)
        if as_dict:
            return my_p.process_documentation.to_dict()
        else:
            return my_p.process_documentation

    def get_process_id(self, uuid=None):
        """Return a process identifier.

        If no ID is provided, attempts to find and return the first reference
        process associated with a project's product systems.

        If a product system ID is sent, then return the reference process
        associated with it.

        If a process ID is sent, it gets returned unchanged.

        Parameters
        ----------
        uuid : str, optional
            Universally unique identifier (UUID), by default None

        Returns
        -------
        str/NoneType
            UUID of a process (or reference process to a product system);
            otherwise, NoneType.
        """
        rp_uid = None
        if uuid is None or uuid == "":
            self.logger.info("Searching for reference process")
            try:
                rp_uid = self.get_reference_process_id()[0]
            except:
                self.logger.warning("Failed to find reference process ID!")
        elif uuid in self.get_spec_ids(o.ProductSystem):
            self.logger.debug(
                "Querying reference process for product system '%s'" % uuid)
            try:
                rp_uid = self.query(o.ProductSystem, uuid).ref_process.id
            except:
                self.logger.info("Assuming first reference process")
                rp_uid = self.get_reference_process_id(uuid)[0]
        elif uuid in self.get_spec_ids(o.Process):
            rp_uid = uuid
        return rp_uid

    def get_process_sources(self, uuid):
        s_list = []
        if uuid in self.get_spec_ids(o.Process):
            obj = self.query(o.Process, uuid)
            if obj.process_documentation.sources:
                s_list += obj.process_documentation.sources

        return s_list

    def get_sources(self, uuid, all_p=False):
        # Sources are bound to a process's documentation attribute.
        # There are two routes: the reference process or all processes.
        s_list = []
        if uuid in self.get_spec_ids(o.ProductSystem):
            if all_p:
                self.logger.info("Reading all sources for product system")
                ps = self.query(o.ProductSystem, uuid)
                for obj in ps.processes:
                    p_list = self.get_process_sources(obj.id)
                    s_list += p_list
            else:
                self.logger.info("Reading sources for reference process.")
                obj_id = self.get_process_id(uuid)
                p_list = self.get_process_sources(obj_id)
                s_list += p_list
        elif uuid in self.get_spec_ids(o.Process):
            self.logger.info("Reading process sources")
            p_list = self.get_process_sources(uuid)
            s_list += p_list

        # Remove duplicates
        s_ids = []
        s_objs = []
        for source in s_list:
            if source.id not in s_ids:
                s_ids.append(source.id)
                # Note that sources are Ref objects, get class object.
                source = self.query(o.Source, source.id)
                s_objs.append(source)

        return s_objs

    def get_providers(self):
        """Return a list of technosphere flow provider and flow information.

        Note
        ----
        This method is only available for openLCA 2.0 client connections.

        Returns
        -------
        list
            A list of TechFlow objects.
        """
        r_list = []
        if self.client:
            try:
                r_list = self.client.get_providers()
            except ConnectionRefusedError as e:
                self.logger.warning(
                    "Server may have stopped! "
                    "Please check connection.\n%s" % e)
            else:
                self.logger.debug("Found %d providers." % len(r_list))
        else:
            self.logger.warning("No connection!")
        return r_list

    def get_reviewer(self, uuid=None):
        """Return tuple of current unit process reviewer.

        Parameters
        ----------
        uuid : str, optional
            Universally unique identifier (UUID) for a process, by default None

        Returns
        -------
        tuple[str, str]
            Reviewer name and associated actor's UUID.
            If no reviewer, then tuple values are both NoneType.
        """
        r_tup = (None, None)
        rp_id = self.get_process_id(uuid)
        if rp_id is not None:
            p_obj = self.get_process_doc(uuid=rp_id, as_dict=False)
            # Reviewer attribute is a Ref object to an Actor
            r = p_obj.reviewer
            if r is not None:
                r_name = "%s" % r.name
                r_id =  "%s" % r.id
                r_tup = (r_name, r_id)
        return r_tup

    def get_reference_category(self, uuid=None):
        """Return the category associated with a reference process.

        Notes
        -----
        Will return the category for any process if a process UUID is sent
        or an empty string if the UUID is not from a product system

        Parameters
        ----------
        uuid : str, optional
            Universally unique identifier (UUID), by default None

        Returns
        -------
        str
            Category description.
        """
        r_str = ""
        rp_uid = self.get_process_id(uuid)
        if rp_uid is not None:
            rp = self.query(o.Process, rp_uid)
            if rp is not None:
                r_str += "%s" % rp.category
        return r_str

    def get_reference_description(self, uuid=None):
        """Return the description associated with a reference process.

        Notes
        -----
        Will return the description for any process if a process UUID is sent
        or an empty string if the UUID is not from a product system

        Parameters
        ----------
        uuid : str, optional
            Universally unique identifier (UUID), by default None

        Returns
        -------
        str
            Process description.
        """
        r_str = ""
        rp_uid = self.get_process_id(uuid)
        if rp_uid is not None:
            rp = self.query(o.Process, rp_uid)
            if rp is not None:
                r_str += "%s" % rp.description
        return r_str

    def get_reference_doc(self, uuid=None):
        """Return the reference process documentation.

        Notes
        -----
        Will return the documentation for any process if a process UUID is sent
        or an empty string if the UUID is not from a product system.

        Parameters
        ----------
        uuid : str, optional
            Universally unique identifier (UUID), by default None

        Returns
        -------
        str
            Process documentation in YAML format.
            By design the olca-schema keys that begin with "@" sign are
            removed from the string.
        """
        r_str = ""
        rp_uid = self.get_process_id(uuid)
        if rp_uid is not None:
            r_dict = self.get_process_doc(uuid=rp_uid, as_dict=True)
            r_str = get_as_yaml(r_dict, rm_at=True)
        return r_str

    def get_reference_flow(self, uuid):
        """Return a string of the reference flow.

        Note that the product system's target amount and target unit are
        used in conjunction with the process's quantitative reference flow's
        name.

        Furthermore, note that this target amount and target unit may be
        different from the quantitative reference flow's amount and unit
        found in the exchange table. For example, the UP is based on 100 g
        reference flow, but the product system's functional unit is set to
        1 kg.

        Parameters
        ----------
        uuid : str
            Product system UUID representing the unit process.

        Returns
        -------
        str
            Reference flow amount of units of flow name.

        Raises
        ------
        ValueError
            When product system UUID not received.
        """
        if not uuid in self.get_spec_ids(o.ProductSystem):
            raise ValueError("UUID must be for a product system!")
        rp_id = self.get_reference_process_id(uuid=uuid)[0]
        ps = self.query(o.ProductSystem, d_uuid=uuid)
        re_id = ps.ref_exchange.internal_id
        my_ex = self.find_ref_exchange(rp_id, re_id)
        name = my_ex.flow.name
        my_str = "%s %s of %s" % (ps.target_amount, ps.target_unit.name, name)
        return my_str

    def get_reference_name(self, uuid=None):
        """Return the reference process name.

        Notes
        -----
        Will return the name for any process if a process UUID is sent
        or an empty string if the UUID is not from a product system

        Parameters
        ----------
        uuid : str, optional
            Universally unique identifier (UUID), by default None

        Returns
        -------
        str
            Process name.
        """
        r_str = ""
        rp_uid = self.get_process_id(uuid)
        if rp_uid is not None:
            rp = self.query(o.Process, rp_uid)
            if rp is not None:
                r_str += "%s" % rp.name
        return r_str

    def get_reference_process(self, uuid=None, as_ref=True):
        """Return a list of reference processes for a given product system.

        If not UUID is given, then the Product System IDs are queried for a
        reference process.

        Parameters
        ----------
        uuid : str, optional
            A product system UUID. Defaults to None.
        as_ref: bool, optional
            Switch for return object types found in list.
            If true, list types are Ref objects; otherwise, list types are
            Process objects. Defaults to True.

        Returns
        -------
        list
            List of reference processes (either Ref or Process schema object
            types).

        See also
        --------
        https://greendelta.github.io/olca-schema/classes/ProductSystem.html
        """
        uid_list = []
        p_list = []
        if uuid is None:
            self.logger.info("Reading UUIDs for product systems")
            uid_list = self.get_spec_ids(o.ProductSystem)
        else:
            uid_list.append(uuid)
        if len(uid_list) > 1:
            self.logger.warning(
                "Found %d product systems! Expected one!" % len(uid_list))
        for id_ in uid_list:
            if id_ is not None or id_ != "":
                ps = self.query(o.ProductSystem, id_)
                ref_p = ps.ref_process
                if as_ref:
                    p_list.append(ref_p)
                else:
                    full_p = self.query(o.Process, ref_p.id)
                    p_list.append(full_p)
        return p_list

    def get_reference_process_id(self, uuid=None):
        """Same as :func:`get_reference_process`, but the return list contains
        only UUID strings.

        Parameters
        ----------
        uuid : str, optional
            A product system's UUID. Defaults to None.

        Returns
        -------
        list
            List of UUIDs of reference processes.
        """
        ps_list = self.get_reference_process(uuid)
        ps_list = [ps.id for ps in ps_list]
        return ps_list

    def get_spec_class(self, name):
        """Return olca_schema.schema object type for a given name.

        This is a helper method to avoid having to import olca_schema to
        get access to the objects. Just pass this method a name, and out
        comes the associated object.

        Parameters
        ----------
        name : str
            Name (or display name) for a root entity schema class.

        Raises
        ------
        ValueError
            If name or display name is not found.
        KeyError
            If name or display name key is nto found.

        Returns
        -------
        olca_schema.schema object
            A schema object.

        Examples
        --------
        >>> netl = NetlOlca()
        >>> netl.get_spec_class("Process") # return class
        olca_schema.schema.Process
        >>> netl.get_spec_class("Actor")() # return class instance
        Actor(id='e7e6a683-24ac-4c75-a040-d972d4e506fc', address=None,
        category=None, city=None, country=None, description=None, email=None,
        last_change='2023-08-29T18:15:27.777042Z', name=None, tags=None,
        telefax=None, telephone=None, version='01.00.000', website=None,
        zip_code=None)
        """
        if name in self.spec_names:
            d_key = get_dict_number(self._spec_map, name, "name")
        elif isinstance(name, str) and name.lower() in self.spec_names:
            d_key = get_dict_number(self._spec_map, name.lower(), "name")
        elif name in self.spec_display_names:
            d_key = get_dict_number(self._spec_map, name, "display")
        else:
            raise ValueError("Name, '%s', not found!" % name)
        if d_key is not None:
            return self._spec_map[d_key]["class"]
        else:
            raise KeyError("Failed to find name '%s'" % name)

    def get_spec_ids(self, d_type):
        """Return the ID list for a given data set type.

        Parameters
        ----------
        d_type : olca_schema.schema.*
            A schema class.

        Returns
        -------
        list
            List of UUIDs.
        """
        return self._get_from_spec_class(d_type, 'ids')

    def get_spec_name(self, d_type):
        """Return the name for a given olca_schema class type.

        Parameters
        ----------
        d_type : olca_schema object
            A root entity olca_schema class object.

        Returns
        -------
        str
            The name for a given class type.
        """
        return self._get_from_spec_class(d_type, 'name')

    def get_yaml_entities(self, fpath):
        """Read YAML file for olca.schema entries and return as list.

        Notes
        -----
        For some reason, the zip_code field is not registering for Actors,
        so changed to 'zipCode' and now it works.

        Parameters
        ----------
        fpath : str
            A valid file path to a YAML olca.schema template.
            See :func:`make_actor_yaml` as an example.

        Returns
        -------
        list
            A list of schema objects.
        """
        r_list = []

        # The YAML dict keys are the name of an olca schema root entities
        # (e.g., 'actors') and the value of each key is a list of entries.
        f_dict = read_yaml(fpath)

        # Get schema entity name and its list of entries
        for s_name, s_list in f_dict.items():
            # Each item in the values list is a dictionary where the keys
            # are class attribute names.
            for i_dict in s_list:
                # Initialize new schema object
                try:
                    d_obj = self.get_spec_class(s_name).from_dict(i_dict)
                except Exception as e:
                    self.logger.error(
                        "Failed to read '%s' olca.schema from YAML file "
                        "%s" % (s_name, fpath))
                    self.logger.debug("%s" % str(e))
                else:
                    r_list.append(d_obj)
        return r_list

    def list_parameters(self,
                        inc_global=True,
                        inc_process=True,
                        input_only=False,
                        as_dict=False):
        # IN PROGRESS
        # To get a master list of global and/or process parameters filterable
        # by input parameter status.
        param_list = []

        # Global scope
        if inc_global:
            for par_id in self.get_spec_ids(o.Parameter):
                par_obj = self.query(o.Parameter, par_id)
                if input_only and par_obj.is_input_parameter:
                    if as_dict:
                        param_list.append(par_obj.to_dict())
                    else:
                        param_list.append(par_obj)
                elif not input_only:
                    if as_dict:
                        param_list.append(par_obj.to_dict())
                    else:
                        param_list.append(par_obj)

        # Process scope
        if inc_process:
            for p_id in self.get_spec_ids(o.Process):
                p_obj = self.query(o.Process, p_id)
                for par_obj in p_obj.parameters:
                    if input_only and par_obj.is_input_parameter:
                        if as_dict:
                            param_list.append(par_obj.to_dict())
                        else:
                            param_list.append(par_obj)
                    elif not input_only:
                        if as_dict:
                            param_list.append(par_obj.to_dict())
                        else:
                            param_list.append(par_obj)

        return param_list

    def make_exchange(self):
        """Create an empty exchange class

        Returns:
            olca-schema.Exchange: empty Exchange class instance
        """

        return o.Exchange()

    def match_process_names(self, q):
        """Return list of process names and IDs that match query.

        Parameters
        ----------
        q : re.Pattern
            A regular expression pattern object.
            For example: ``q = re.compile("^Electricity; at grid; .*")``

        Returns
        -------
        list
            List of tuples, each tuple of length two or an return empty
            if no processes are found.

            - str: process UUID
            - str: process name (or sub-text from name, if groups defined)
        """
        r_list = []
        my_d = self.get_descriptors(self.get_spec_class("Process"))
        for ref in my_d:
            r = q.match(ref.name)
            if r:
                try:
                    # Pull the search text (if provided)
                    r_list.append(tuple([ref.id, r.group(1)]))
                except IndexError:
                    # If match successful, then group 0 is just the full name
                    r_list.append(tuple([ref.id, r.group(0)]))
        return r_list

    def open(self, jsonld_file):
        """Open a file connection with existing JSON-LD zip.

        Parameters
        ----------
        jsonld_file : str
            A valid openLCA JSON-LD zip file.

        Raises
        ------
        OSErrorr
            For non-existent input files.
        TypeError
            For non-zip archive file formats.
        """
        # HOTFIX: check for zip archive.
        _, ext = os.path.splitext(jsonld_file)
        if ext.lower() != ".zip":
            raise TypeError("JSON-LD file should be a zip archive!")

        if os.path.isfile(jsonld_file):
            self.file = zio.ZipReader(jsonld_file)
            self._filename = jsonld_file
        else:
            raise OSError("Failed to find JSON-LD file, '%s'!" % jsonld_file)

    def print_as_dict(self, d_class):
        """Helper function to pretty-print a schema class as a dictionary.

        Parameters
        ----------
        d_class : olca_schema.schema.* object
            An instantiated schema class.
        """
        if type(d_class).__name__ not in self.schema_class_names:
            raise TypeError("Expected schema class, found %s" % type(d_class))
        pretty_print_dict(d_class.to_dict())

    def print_descriptors(self, d_type=o.FlowProperty, d_prop='name'):
        """Print list of data set descriptor properties.

        Parameters
        ----------
        d_type : olca_schema class (e.g., o.FlowProperty), optional
            The data set type to retrieve descriptors of.
            If no data set type provided, defaults to FlowProperty.
            For a list of available data set types, see "Root entities" here
            https://greendelta.github.io/olca-schema/intro.html.
        d_prop : str
            The data set property value to print.
            Valid values are based on olca_schema.schema.Ref properties
            (e.g., '@type', '@id', 'category', 'description', 'flowType',
            'location', 'name', 'refUnit')
        """
        if self.client or self.file:
            refs = self.get_descriptors(d_type=d_type)
            for ref in refs:
                print(ref.to_dict().get(d_prop))
        else:
            self.logger.warning("No connection!")

    def print_project(self, name='actor'):
        """Print object names for the given data type.

        Warning
        -------
        You probably don't want to run this for data types with huge lists
        of values (e.g., 'flows').

        Parameters
        ----------
        name : str, optional
            The name (or display name) for a given data object type
            (e.g., 'Product system' or 'parameter'). Defaults to 'actor'.
        """
        d_class = self.get_spec_class(name)
        d_disp = self._get_from_spec_class(d_class, 'display')
        d_ids = self._get_from_spec_class(d_class, 'ids')
        n_ids = len(d_ids)
        # NOTE: order of IDs matters! Don't sort.
        print("%s (%d entries):" % (d_disp, n_ids))
        for i in range(n_ids):
            id_ = d_ids[i]
            d_obj = self.query(d_class, id_)
            print("  %d ... %s" % (i+1, d_obj.name))

    def print_providers(self):
        """Print the metadata for flows and their providers.
        """
        providers = self.get_providers()
        for provider in providers:
            # Expected keys are 'flow' and 'provider'
            self.print_as_dict(provider)

    def print_spec_info(self, add_entries=True):
        """Print the keys, display names, and info for each root entity.

        Parameters
        ----------
        add_entries : bool, optional
            Whether to print the number of entries under each entity.
            Defaults to true.
        """
        print("Root entities:")
        for id_, name_, info_ in self.spec_info_tuples:
            print("{0: >2}: {1}\n  {2}".format(id_, name_, info_))
            if add_entries:
                n_ids = len(self._spec_map[id_].get("ids", []))
                print("  %d entries." % n_ids)

    def print_unit_groups(self):
        """Print list of unit group names.
        """
        self.print_descriptors(d_type=o.UnitGroup, d_prop='name')

    def query(self, d_class, d_uuid):
        """Query data handler for a schema object of given type and ID.

        Parameters
        ----------
        d_class : olca_schema.schema object
            A schema object.
        d_uuid : str
            A UUID string.

        Raises
        ------
        ValueError
            If the schema object is unknown.

        Returns
        -------
        olca_schema.schema.* object
            A schema object of give class type associated with the given UUID.
        """
        if d_class not in self.spec_classes:
            raise ValueError("Schema class unknown, %s" % str(d_class))

        r_obj = None
        if self.client:
            self.logger.debug("Querying client for '%s'" % d_uuid)
            r_obj = self.client.get(d_class, d_uuid)
        elif self.file:
            self.logger.debug("Querying file for '%s" % d_uuid)
            r_obj = self.get_from_file(d_class, d_uuid)
        else:
            self.logger.warning("No connection!")
        return r_obj

    def read(self):
        """Read UUIDs from either client or JSON-LD file connection.
        """
        if self.client:
            for spec in self.spec_classes:
                r_list = self.get_descriptors(d_type=spec)
                r_list = [i.id for i in r_list]
                self._add_to_spec_ids(spec, r_list)
                self.logger.debug(
                    "Read %d identifiers to %s." % (len(r_list), str(spec)))
            self.logger.info("Read UUIDs from IPC connection.")
        elif self.file:
            for spec in self.spec_classes:
                r_list = self.file.ids_of(spec)
                self._add_to_spec_ids(spec, r_list)
                self.logger.debug(
                    "Read %d identifiers to %s." % (len(r_list), str(spec)))
            self.logger.info("Read UUIDs from file.")
        else:
            self.logger.warning("No connection opened!")

    def write(self, o_obj, f_path):
        """Write an olca-schema class object to JSON-LD zip file.

        Parameters
        ----------
        o_obj : olca_schema.*
            A root entity defined in olca-schema package.
        f_path : str
            A file path to a JSON-LD file.
            It may also work pointing to a non-existent zip file,
            which should create a new JSON-LD (untested).
        """
        if os.path.isfile(f_path):
            self.logger.warn("Writing to an existing file '%s'" % f_path)
        f = zio.ZipWriter(f_path)
        f.write(o_obj)
        f.close()


##############################################################################
# FUNCTIONS
##############################################################################
def _overwrite(json_file, olca_obj, verbose=True):
    """Overwrite an olca-schema object in an existing JSON-LD file.

    An overwrite to a zip archive requires the file to be extracted,
    the existing data to be overwritten, and the file to be re-zipped.

    Warning
    -------
    For large projects, this will take some time to complete.

    Parameters
    ----------
    json_file : str
        A file path to an existing JSON-LD zip file.
    olca_obj : olca_schema.*
        An olca-schema root entity class to be written to JSON-LD.
    verbose : bool, optional
        If true, displays a progress bar for zip transfer, by default True

    Returns
    -------
    bool
        Whether the data transfer was successful.
        If true, the file located at 'json_file' will be the same as it was
        with the 'olca_obj' data overwritten.
        If false, the original file located at 'json_file' will be untouched
        and a partial data file will be located

    """
    tmp_dir = os.path.expanduser("~")
    tmp_file = "_netlolca_temp.zip"
    tmp_path = os.path.join(tmp_dir, tmp_file)

    # Read all UUIDs from existing JSON-LD zip archive.
    r_data = _read_jsonld(json_file, _root_entity_dict())

    # Initialize the current iteration step and total UUID count.
    cur_num = 0
    tot_num = 0
    for k in r_data.keys():
        tot_num += len(r_data[k]['ids'])

    # Open files for the initial transfer
    r_file = zio.ZipReader(json_file)
    w_file = zio.ZipWriter(tmp_path)
    is_okay = True
    for k in r_data.keys():
        r_class = r_data[k]['class']
        if isinstance(olca_obj, r_class):
            # This is where the overwrite happens!
            # Remove the duplicate entry (or entries).
            for i in [
                x for x, val in enumerate(r_data[k]['ids'])
                if val == olca_obj.id
            ]:
                r_data[k]['ids'].pop(i)

            # Write the new object in its place:
            cur_num += 1
            try:
                w_file.write(olca_obj)
            except:
                logging.warning(
                    "Failed to write %s (%s)!" % (
                        r_data[k]['display'], olca_obj.id)
                )
                is_okay = False

        # Write all other objects to temp file:
        for r_id in r_data[k]['ids']:
            cur_num += 1
            r_obj = r_file.read(r_class, r_id)
            try:
                w_file.write(r_obj)
            except:
                logging.warning(
                    "Failed to write %s (%s)!" % (r_data[k]['display'], r_id)
                )
                is_okay = False
            if verbose:
                print_progress(cur_num, tot_num, "Transferring:")
    # HOTFIX: clean-up step:
    if cur_num < tot_num and verbose:
        print_progress(tot_num, tot_num, "Transferring:")

    if is_okay:
        logging.info("Overwriting JSON-LD")
        try:
            # Give OS a chance...
            os.remove(json_file)
            os.rename(tmp_path, json_file)
        except:
            logging.warning("Something didn't work, trying again...")
            try:
                shutil.move(tmp_path, json_file)
            except:
                logging.error("Overwrite failed!")
            else:
                logging.info("File overwrite complete.")
        else:
            logging.info("File overwrite complete")
    else:
        logging.info(
            "Something went wrong during the data transfer. "
            "Please see original file at '%s'" % json_file)
        logging.info(
            "Partial data transfer to JSON-LD is located '%s'" % tmp_path)

    return is_okay


def _read_jsonld(json_file, root_dict, to_sort=False):
    """Read root entities from JSON-LD file and append to root entity
    dictionary.

    Parameters
    ----------
    json_file : str
        A file path to a zipped JSON-LD file.
    root_dict : dict
        A dictionary with olca-schema root entity data (as provided by
        :func:`_root_entity_dict`).
    to_sort : bool, optional
        Whether to sort the UUID lists (e.g., can speed up searching).
        Defaults to false.

    Returns
    -------
    dict
        The same root entity dictionary with 'ids' and 'objs' lists updated.

    Raises
    ------
    OSError
        If the JSON-LD file path does not exist (or is not a file).

    Examples
    --------
    >>> my_file = "Federal_LCA_Commons-US_electricity_baseline.zip" # 2016 b.l.
    >>> my_dict = _read_jsonld(my_file, _root_entity_dict())
    >>> print('Process:', len(my_dict[11]['ids']), 'UUIDs')
    Process: 606 UUIDs
    """
    if not os.path.isfile(json_file):
        raise OSError("File not found! %s" % json_file)
    else:
        # Create a file handle to the JSON-LD zip
        logging.info("Opening JSON-LD file, %s" % os.path.basename(json_file))
        j_file = zio.ZipReader(json_file)
        for k in root_dict.keys():
            # Get IDs for each root entity
            name = root_dict[k]['display']
            spec = root_dict[k]['class']
            r_ids = j_file.ids_of(spec)
            # HOTFIX: remove duplicate entries (which is an error) and
            # allow sorting, which should speed up the search later on.
            r_ids = list(set(r_ids))
            if to_sort:
                r_ids = sorted(r_ids)
            logging.info("Read %d UUIDs for %s" % (len(r_ids), name))
            root_dict[k]['ids'] = r_ids
        j_file.close()

        return root_dict


def _root_entity_dict():
    """Generate empty dictionary for each openLCA schema root entity.

    Returns
    -------
    dict
        Dictionary with a numeric primary key for each root entity.
        The values are dictionaries with seven keys:

        - 'name': lowercase root entity name
        - 'display': title case root entity name
        - 'info': descriptive text of entity
        - 'class': olca-schema class object
        - 'ids': empty list (for UUIDs of the entity type)
        - 'objs': empty list (for class objects of the entity type)
    """
    return {
        1: {
            'name': 'actor',
            'display': 'Actor',
            'info': 'A person or organization.',
            'class': o.Actor,
            'yaml': "actors.yaml",
            'ids': [],
        },
        2: {
            'name': 'currency',
            'display': "Currency",
            'info': "Costing currency.",
            'class': o.Currency,
            'yaml': None,
            'ids': [],
        },
        3: {
            'name': 'dqsystem',
            'display': 'DQ System',
            'info': "Data quality system, a matrix of quality indicators.",
            'class': o.DQSystem,
            'yaml': None,
            'ids': [],
        },
        4: {
            'name': 'epd',
            'display': 'EPD',
            'info': "Environmental Product Declaration",
            'class': o.Epd,
            'yaml': None,
            'ids': [],
        },
        5: {
            'name': "flow",
            'display': "Flow",
            'info': "Everything that can be an input/output of a process.",
            'class': o.Flow,
            'yaml': None,
            'ids': [],
        },
        6: {
            'name': "flowproperty",
            'display': "Flow property",
            'info': "Quantity used to express amounts of flow.",
            'class': o.FlowProperty,
            'yaml': None,
            'ids': [],
        },
        7: {
            'name': "impactcategory",
            'display': "Impact category",
            'info': "Life cycle impact assessment category.",
            'class': o.ImpactCategory,
            'yaml': None,
            'ids': [],
        },
        8: {
            'name': "impactmethod",
            'display': "Impact method",
            'info': "An impact assessment method.",
            'class': o.ImpactMethod,
            'yaml': None,
            'ids': [],
        },
        9: {
            'name': "location",
            'display': "Location",
            'info': "A location (e.g., country, state, city).",
            'class': o.Location,
            'yaml': None,
            'ids': [],
        },
        10: {
            'name': "parameter",
            'display': "Parameter",
            'info': "Input or dependent global/process/impact parameter.",
            'class': o.Parameter,
            'yaml': None,
            'ids': [],
        },
        11: {
            'name': "process",
            'display': "Process",
            'info': "Systematic organization or series of actions.",
            'class': o.Process,
            'yaml': None,
            'ids': [],
        },
        12: {
            'name': 'productsystem',
            'display': "Product system",
            'info': "A product's supply chain (functional unit).",
            'class': o.ProductSystem,
            'yaml': None,
            'ids': [],
        },
        13: {
            'name': "project",
            'display': "Project",
            'info': "An openLCA project.",
            'class': o.Project,
            'yaml': None,
            'ids': [],
        },
        14: {
            'name': "result",
            'display': "Result",
            'info': "A calculation result of a product system.",
            'class': o.Result,
            'yaml': None,
            'ids': [],
        },
        15: {
            'name': "socialindicator",
            'display': "Social indicator",
            'info': "An indicator for Social LCA.",
            'class': o.SocialIndicator,
            'yaml': None,
            'ids': [],
        },
        16 : {
            'name': 'source',
            'display': "Source",
            'info': "A literature reference.",
            'class': o.Source,
            'yaml': None,
            'ids': [],
        },
        17: {
            'name': "unitgroup",
            'display': "Unit group",
            'info': "Group of units that can be inter-converted.",
            'class': o.UnitGroup,
            'yaml': None,
            'ids': [],
        }
    }


def check_for_docker():
    """Based on the quay.io Jupyter Docker images, the default user is
    jovyan, so check the environment variable against this "unique"
    string for a probable docker environment.

    Returns
    -------
    bool
        Guess whether class is executed within a Docker container.

    Notes
    -----
    Reference:
    https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html
    """
    if os.environ.get("USER") == "jovyan":
        return True
    else:
        return False


def get_as_yaml(my_dict, rm_at=False):
    """Return a dictionary as a YAML string.

    Parameters
    ----------
    my_dict : dict
        A dictionary object.
    rm_at : bool, optional
        Whether to remove lines begining with '@' sign.
        For example, olca-schema class objects, when dumped to dictionary
        may have dictionary keys such as '@type' and '@id' that are not
        desired for display. Set true to remove these lines from the
        YAML string. This is handled using string substitution provided
        by regular expressions package, re.

    Returns
    -------
    str
        YAML-formatted string based on the dictionary provided.

    Raises
    ------
    TypeError
        When parameter type is not dictionary.
    """
    y_txt = ""
    if isinstance(my_dict, dict):
        y_txt = yaml.dump(my_dict, sort_keys=False)
    else:
        raise TypeError("Expected a dictionary, found %s" % type(my_dict))
    if rm_at:
        # Removes the "'@type'; Words, words\n  " and similar from text
        p = re.compile("\'@.*\': .*\n\\s{2}")
        y_txt = p.sub("", y_txt)
    return y_txt


def get_dict_number(dobj, val, key):
    """Return the top-level dictionary key for a second-level dictionary
    given its value for a given key.

    Args:
        dobj (dict):
            A dictionary of dictionaries.
            Its key is the desired return value.
        val (str):
            The desired matching value of the inner dictionary.
        key (str):
            The key for searching the values within the inner dictionary.

    Returns:
        int: The outer dictionary key, which is intended to be an integer,
        but it doesn't have to be. If not found, returns NoneType.
    """
    for idx, mini_dict in dobj.items():
        if isinstance(mini_dict, dict):
            name = mini_dict.get(key, "qwerty1234,./")
            if name == val:
                return idx
    return None


def make_actor_yaml(yaml_name, yaml_dir="data"):
    """Create a YAML file with actor template.

    Parameters
    ----------
    yaml_name : str
        The file name for the actors template (e.g., "actors.yaml").
    yaml_dir : str, optional
        A directory path to create the actors.yaml template file.

    Notes
    -----
    This YAML is based on Version 2 of openLCA's schema
    https://greendelta.github.io/olca-schema/classes/Actor.html
    but omits 'id', 'last_change' and 'version, which are generated on-the-fly
    and 'category', which doesn't make sense in this context.

    The YAML file names are now available in NetlOlca class attribute,
    _spec_map, in the 'yaml' sub-field.
    """
    yaml_path = os.path.join(yaml_dir, yaml_name)
    if os.path.isfile(yaml_path):
        logging.warn("YAML actors.yaml already exists; overwriting!")

    # Instructions to be included as top-level comments in the YAML file.
    i_txt = """# NETL: Unit Process Template
# actors.yaml
# -----------------------------------------------------------------------
# INSTRUCTIONS:
# This YAML provides a list of external actors (people and organizations)
# to be read by NetlOlca Python class for modifying openLCA projects.
#
# The structure of this YAML is based on Version 2 of openLCA's schema
# https://greendelta.github.io/olca-schema/classes/Actor.html
#
# "address" required; should follow guidelines for postal addressing.
#   For example, in the US, use street, city, state; you may omit
#   name and zip code, as they appear in separate fields.
# "city" optional; may be omitted if included in address.
#   If city, state are omitted from address, include here.
# "country" required; should use ISO 3166 official name, 2 or 3 alpha code.
#   https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes
# "email" optional; should be in a valid format (e.g., <local>@<domain>)
# "name" required; should include first and last name (if available)
#   or full business name.
# "tags" are a list of strings that should not contain any commas or
#   special characters (e.g., ~`!@#$%^&*()+=|:;,.?'"/).
# "telefax" optional; include area code and country code if different
#   from country field.
# "telephone" required, include area code and country code if different
#   from country field.
# "website" optional; should be a valid web address including the scheme
#   (e.g., HTTP or HTTPS), domain, and any paths
# "zipCode" optional; if not in address, it should be ZIP+4 format
#
"""
    yt_dict = {
        "actor": [
            {"address": "",
             "city": "",
             "country": "US",
             "description": "",
             "email": "",
             "name": "",
             "tags": [],
             "telefax": "",
             "telephone": "",
             "website": "",
             "zipCode": ""},]
    }
    yt_txt = get_as_yaml(yt_dict)
    d_txt = i_txt + yt_txt
    writeout(yaml_path, d_txt)


def pretty_print_dict(dict_obj, ind=2):
    """Pretty-prints a dictionary using json dumps.

    Args:
        dict_obj (dict):
            A dictionary object.
        ind (int, optional):
            The number of indentation spaces. Defaults to 2.

    Returns:
        None

    Raises:
        TypeError: An invalid parameter received.
    """
    if isinstance(dict_obj, dict):
        if isinstance(ind, int):
            print(json.dumps(dict_obj, indent=ind, default=str))
        else:
            raise TypeError("Expected integer, got '%s'" % type(ind))
    else:
        raise TypeError("Expected dictionary, got '%s'" % type(dict_obj))


def print_progress(iteration, total, prefix='', suffix='', decimals=0,
                   bar_length=44):
    """Create a terminal progress bar.

    Parameters
    ----------
    iteration : int
        Current iteration.
    total : int
        Total iterations.
    prefix : str, optional
        Prefix string, defaults to empty string.
    suffix : str, optional
        Suffix string, defaults to empty string.
    decimals : int, optional
        The number of decimal places to display in percent complete.
        Defaults to zero (i.e., whole integer).
    bar_length : int, optional
        The character length of the progress bar, defaults to 44.

    Notes
    -----
    Reference:
        "Python Progress Bar" by Aubrey Taylor (c) 2020.
        https://gist.github.com/aubricus/f91fb55dc6ba5557fbab06119420dd6a
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write(
        '\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


def read_yaml(fpath):
    """Read and return YAML file as a Python dictionary.

    Parameters
    ----------
    fpath : str
        A valid file path to an existing YAML file.

    Returns
    -------
    dict
        A dictionary of the YAML file contents.
    """
    c_dict = {}
    if os.path.isfile(fpath):
        with open(fpath, 'r') as f:
            try:
                c_dict = yaml.safe_load(f)
            except yaml.YAMLError:
                logging.error("Failed to read YAML from %s" % fpath)
    return c_dict


def writeout(fpath, dstring):
    """Write new/overwrite existing file with given data string.

    Notes
    -----
    If successful, the output file will exist.
    This allows a system check for file existence for futher editing.

    Warning
    -------
    Overwrites existing files deleting all previous content!

    Parameters
    ----------
    fpath : str
        A valid file path.
    dstring : str
        Data string to be written to file.
    """
    try:
        OUT = open(fpath, 'w')
        OUT.write(dstring)
    except:
        pass
    else:
        OUT.close()
