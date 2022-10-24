# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
"""Invenio module for information retrieval."""

from __future__ import absolute_import, print_function

import errno
import json
import os
import warnings

from pkg_resources import iter_entry_points, resource_filename, \
    resource_isdir, resource_listdir
from werkzeug.utils import cached_property, import_string

from . import config
from .cli import index as index_cmd
from .engine import ES, OS, SEARCH_DISTRIBUTION, SearchEngine, search
from .errors import IndexAlreadyExistsError
from .utils import build_alias_name, build_index_from_parts, \
    build_index_name, timestamp_suffix


class _SearchState(object):
    """Store connection to elastic client and registered indexes."""
    def __init__(self,
                 app,
                 entry_point_group_mappings=None,
                 entry_point_group_templates=None,
                 **kwargs):
        """Initialize state.

        :param app: An instance of :class:`~flask.app.Flask`.
        :param entry_point_group_mappings:
            The entrypoint group name to load mappings.
        :param entry_point_group_templates:
            The entrypoint group name to load templates.
        """
        self.app = app
        self.aliases = {}
        self._client = kwargs.get('client')
        self.entry_point_group_templates = entry_point_group_templates
        self._current_suffix = None

        if entry_point_group_mappings:
            self.load_entry_point_group_mappings(entry_point_group_mappings)

        with app.app_context():
            get_mappings = app.config.get('SEARCH_GET_MAPPINGS_IMP')
            if get_mappings:
                self.get_mappings = import_string(get_mappings)

    def __getattr__(self, name):
        """Call get_mappings() method on mappings retrieval."""
        if name == 'mappings':
            return self.get_mappings()
        else:
            raise AttributeError

    def get_mappings(self):
        """Default get_mappings imp, return empty object."""
        return {}

    @property
    def current_suffix(self):
        """Return the current suffix."""
        if self._current_suffix is None:
            self._current_suffix = timestamp_suffix()
        return self._current_suffix

    @cached_property
    def templates(self):
        """Generate a dictionary with template names and file paths."""
        templates = {}
        result = []
        if self.entry_point_group_templates:
            result = self.load_entry_point_group_templates(
                self.entry_point_group_templates) or []

        for template in result:
            for name, path in template.items():
                templates[name] = path

        return templates

    @staticmethod
    def _get_mappings_module(module):
        """Resolves the module where to find search mappings/templates.
        Finds the module that contains the search mappings/templates based
        on the current installed search distribution.
        :param module: the module/package name.
        """
        search_major_version = search.VERSION[0]
        if SEARCH_DISTRIBUTION == ES:
            subfolder = "v{}".format(search_major_version)
        elif SEARCH_DISTRIBUTION == OS:
            subfolder = "os-v{}".format(search_major_version)

            # Make sure that the OpenSearch mappings are in the folder.
            # The fallback can be removed after transition to OpenSearch.
            try:
                resource_listdir(module, subfolder)
            except FileNotFoundError:
                # fallback to ES folder with a warning if `os-vx` is not found
                subfolder = "v7"
                warnings.warn(
                    "OpenSearch v{version} mappings files not found, falling back to Elasticsearch v7 mappings for module {module}. Please add the missing OpenSearch os-v{version} mappings.".format(
                        module=module,
                        version=search_major_version,
                    )
                )
        else:
            # should never happen
            raise RuntimeError(
                "Unknown search distribution {}".format(SEARCH_DISTRIBUTION)
            )

        return "{}.{}".format(module, subfolder)

    def register_mappings(self, alias, package_name):
        """Register mappings from a package under given alias.

        :param alias: The alias.
        :param package_name: The package name.
        """
        package_name = self._get_mappings_module(package_name)

        def _walk_dir(aliases, *parts):
            root_name = build_index_from_parts(*parts)
            resource_name = os.path.join(*parts)

            data = aliases.get(root_name, {})

            for filename in resource_listdir(package_name, resource_name):
                file_path = os.path.join(resource_name, filename)

                if resource_isdir(package_name, file_path):
                    _walk_dir(data, *(parts + (filename, )))
                    continue

                filename_root, ext = os.path.splitext(filename)
                if ext not in {'.json', }:
                    continue

                index_name = build_index_from_parts(
                    *(parts + (filename_root, ))
                )
                assert index_name not in data, 'Duplicate index'
                filename = resource_filename(
                    package_name, os.path.join(resource_name, filename))
                data[index_name] = filename
                self.mappings[index_name] = filename

            aliases[root_name] = data

        # Start the recursion here:
        _walk_dir(self.aliases, alias)

    def register_templates(self, directory):
        """Register templates from the provided directory.

        :param directory: The templates directory.
        """
        directory = self._get_mappings_module(directory)
        result = {}
        module_name, parts = directory.split('.')[0], directory.split('.')[1:]
        parts = tuple(parts)

        def _walk_dir(parts):
            resource_name = os.path.join(*parts)

            for filename in resource_listdir(module_name, resource_name):
                file_path = os.path.join(resource_name, filename)

                if resource_isdir(module_name, file_path):
                    _walk_dir((parts + (filename, )))
                    continue

                filename_root, ext = os.path.splitext(filename)
                if ext not in {'.json', }:
                    continue

                template_name = build_index_from_parts(
                    *(parts[1:] + (filename_root, ))
                )
                result[template_name] = resource_filename(
                    module_name, os.path.join(resource_name, filename))

        # Start the recursion here:
        _walk_dir(parts)
        return result

    def load_entry_point_group_mappings(self, entry_point_group_mappings):
        """Load actions from an entry point group."""
        for ep in iter_entry_points(group=entry_point_group_mappings):
            self.register_mappings(ep.name, ep.module_name)

    def load_entry_point_group_templates(self, entry_point_group_templates):
        """Load actions from an entry point group."""
        result = []
        for ep in iter_entry_points(group=entry_point_group_templates):
            with self.app.app_context():
                for template_dir in ep.load()():
                    result.append(self.register_templates(template_dir))
        return result

    def _client_builder(self):
        """Build Elasticsearch client."""
        client_config = self.app.config.get('SEARCH_CLIENT_CONFIG') or {}
        client_config.setdefault(
            'hosts', self.app.config.get('SEARCH_ELASTIC_HOSTS'))
        return SearchEngine(**client_config)

    @property
    def client(self):
        """Return client for current application."""
        if self._client is None:
            self._client = self._client_builder()
        return self._client

    def flush_and_refresh(self, index):
        """Flush and refresh one or more indices.

        .. warning::

           Do not call this method unless you know what you are doing. This
           method is only intended to be called during tests.
        """
        prefixed_index = build_alias_name(index, app=self.app)
        self.client.indices.flush(wait_if_ongoing=True, index=prefixed_index)
        self.client.indices.refresh(index=prefixed_index)
        self.client.cluster.health(
            wait_for_status='yellow', request_timeout=30)
        return True

    @property
    def cluster_version(self):
        """Get version of Elasticsearch running on the cluster."""
        versionstr = self.client.info()['version']['number']
        return [int(x) for x in versionstr.split('.')]

    @property
    def cluster_distribution(self):
        """Get the distribution entry (opensearch or elasticsearch) on the cluster."""
        # OpenSearch provides a "distribution" field containing "opensearch"
        # Elasticsearch doesn't seem to do that
        # (checked versions: 7.10.2, 7.11.2, 7.17.5, 8.3.1)
        return self.client.info()["version"].get("distribution", "elasticsearch")

    @property
    def active_aliases(self):
        """Get a filtered list of aliases based on configuration.

        Returns aliases and their mappings that are defined in the
        `SEARCH_MAPPINGS` config variable. If the `SEARCH_MAPPINGS` is set to
        `None` (the default), all aliases are included.
        """
        whitelisted_aliases = self.app.config.get('SEARCH_MAPPINGS')
        if whitelisted_aliases is None:
            return self.aliases
        else:
            return {
                k: v
                for k, v in self.aliases.items() if k in whitelisted_aliases
            }

    def _get_indices(self, tree_or_filename):
        for name, value in tree_or_filename.items():
            if isinstance(value, dict):
                for result in self._get_indices(value):
                    yield result
            else:
                yield name

    def create_index(self, index, mapping_path=None, prefix=None, suffix=None,
                     create_write_alias=True, ignore=None, dry_run=False):
        """Create index with a write alias."""
        mapping_path = mapping_path or self.mappings[index]

        final_alias = None
        final_index = None
        index_result = None, None
        alias_result = None, None
        # To prevent index init --force from creating a suffixed
        # index if the current instance is running without suffixes
        # make sure there is no index with the same name as the
        # alias name (i.e. the index name without the suffix).
        with open(mapping_path, 'r') as body:
            final_index = build_index_name(
                index, prefix=prefix, suffix=suffix, app=self.app)
            if create_write_alias:
                final_alias = build_alias_name(
                    index, prefix=prefix, app=self.app)
            index_result = (
                final_index,
                self.client.indices.create(
                    index=final_index,
                    body=json.load(body),
                    ignore=ignore,
                ) if not dry_run else None
            )
            if create_write_alias:
                alias_result = (
                    final_alias,
                    self.client.indices.put_alias(
                        index=final_index,
                        name=final_alias,
                        ignore=ignore,
                    ) if not dry_run else None
                )
        return index_result, alias_result

    def create(self, ignore=None):
        """Yield tuple with created index name and responses from a client."""
        ignore = ignore or []

        def _create(tree_or_filename, alias=None):
            """Create indices and aliases by walking DFS."""
            # Iterate over aliases:
            for name, value in tree_or_filename.items():
                if isinstance(value, dict):
                    for result in _create(value, alias=name):
                        yield result
                else:
                    with open(value, 'r') as body:
                        yield name, self.client.indices.create(
                            index=name,
                            body=json.load(body),
                            ignore=ignore,
                        )

            if alias:
                yield alias, self.client.indices.put_alias(
                    index=list(self._get_indices(tree_or_filename)),
                    name=alias,
                    ignore=ignore,
                )

        for result in _create(self.active_aliases):
            yield result

    def put_templates(self, ignore=None):
        """Yield tuple with registered template and response from client."""
        ignore = ignore or []

        def _replace_prefix(template_path, body):
            """Replace index prefix in template request body."""
            pattern = '__SEARCH_INDEX_PREFIX__'

            prefix = self.app.config['SEARCH_INDEX_PREFIX'] or ''
            if prefix:
                assert pattern in body, "You are using the prefix `{0}`, "
                "but the template `{1}` does not contain the "
                "pattern `{2}`.".format(prefix, template_path, pattern)

            return body.replace(pattern, prefix)

        def _put_template(template):
            """Put template in search client."""
            with open(self.templates[template], 'r') as fp:
                body = fp.read()
                replaced_body = _replace_prefix(self.templates[template], body)
                template_name = build_alias_name(template, app=self.app)
                return self.templates[template],\
                    self.client.indices.put_template(
                        name=template_name,
                        body=json.loads(replaced_body),
                        ignore=ignore,
                )

        for template in self.templates:
            yield _put_template(template)

    def delete(self, ignore=None):
        """Yield tuple with deleted index name and responses from a client."""
        ignore = ignore or []

        def _delete(tree_or_filename, alias=None):
            """Delete indexes and aliases by walking DFS."""
            # Iterate over aliases:
            for name, value in tree_or_filename.items():
                if isinstance(value, dict):
                    for result in _delete(value, alias=name):
                        yield result
                else:
                    # Resolve values to suffixed (or not) indices
                    prefixed_index = build_alias_name(name, app=self.app)
                    lookup_response = self.client.indices.get_alias(
                        index=prefixed_index, ignore=[404])
                    if 'error' in lookup_response:
                        indices_to_delete = []
                    else:
                        indices_to_delete = list(lookup_response.keys())
                    if len(indices_to_delete) == 0:
                        pass
                    elif len(indices_to_delete) == 1:
                        yield name, self.client.indices.delete(
                            index=indices_to_delete[0],
                            ignore=ignore,
                        )
                    else:
                        warnings.warn((
                            'Multiple indices found during deletion of '
                            '{name}: {indices}. Deletion was skipped for them.'
                        ).format(name=name, indices=indices_to_delete))

        for result in _delete(self.active_aliases):
            yield result


class InvenioSearch(object):
    """Invenio-Search extension."""
    def __init__(self, app=None, **kwargs):
        """Extension initialization.

        :param app: An instance of :class:`~flask.app.Flask`.
        """
        self._clients = {}

        if app:
            self.init_app(app, **kwargs)

    def init_app(self,
                 app,
                 entry_point_group_mappings='invenio_search.mappings',
                 entry_point_group_templates='invenio_search.templates',
                 **kwargs):
        """Flask application initialization.

        :param app: An instance of :class:`~flask.app.Flask`.
        """
        self.init_config(app)

        app.cli.add_command(index_cmd)

        state = _SearchState(
            app,
            entry_point_group_mappings=entry_point_group_mappings,
            entry_point_group_templates=entry_point_group_templates,
            **kwargs)
        self._state = app.extensions['invenio-search'] = state

    @staticmethod
    def init_config(app):
        """Initialize configuration.

        :param app: An instance of :class:`~flask.app.Flask`.
        """
        for k in dir(config):
            if k.startswith('SEARCH_'):
                app.config.setdefault(k, getattr(config, k))

    def __getattr__(self, name):
        """Proxy to state object."""
        return getattr(self._state, name, None)
