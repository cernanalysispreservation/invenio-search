# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

import pytest
from mock import patch

from invenio_search.utils import build_index_name, schema_to_index


@pytest.mark.parametrize(
    ('schema, expected, index_names'),
    (
        (
            'records/record-v1.0.0.json',
            ('records-record-v1.0.0', 'record-v1.0.0'),
            None,
        ),
        (
            '/records/record-v1.0.0.json',
            ('records-record-v1.0.0', 'record-v1.0.0'),
            None,
        ),
        (
            'default-v1.0.0.json',
            ('default-v1.0.0', 'default-v1.0.0'),
            None,
        ),
        (
            'default-v1.0.0.json',
            (None, None),
            [],
        ),
        (
            'invalidextension',
            (None, None),
            None,
        ),
    ),
)
def test_schema_to_index(schema, expected, index_names, app):
    """Test the expected value of schema to index."""
    result = schema_to_index(schema, index_names=index_names)
    assert result == expected


@pytest.mark.parametrize(('prefix', 'expected'), [
    ('test-', ('test-default-v1.0.0', 'default-v1.0.0')),
    (None, ('abc-default-v1.0.0', 'default-v1.0.0')),
    ('', ('default-v1.0.0', 'default-v1.0.0')),
])
def test_schema_to_index_prefixes_indices(app, prefix, expected):
    """Test that prefix is added to the index when creating it."""
    new_conf = {'SEARCH_INDEX_PREFIX': 'abc-'}
    with patch.dict(app.config, new_conf):
        result = schema_to_index('default-v1.0.0.json', prefix=prefix)

        assert result == expected


@pytest.mark.parametrize(('parts', 'prefix', 'suffix', 'expected'), [
    (['records'], '', '', 'records'),
    (['records'], 'foo-', '', 'foo-records'),
    (['records', 'record'], 'foo-', '', 'foo-records-record'),
    (['records', 'record'], '', '-new', 'records-record-new'),
    (['test', 'recs', 'rec'], 'foo-', '-old', 'foo-test-recs-rec-old'),
])
def test_build_suffix_index_name(app, parts, prefix, suffix, expected):
    app.config.update(SEARCH_INDEX_PREFIX=prefix)
    assert build_index_name(parts, suffix=suffix, app=app) == expected


@pytest.mark.parametrize(('index_names', 'prefix', 'expected'), [
    (
        ['default-v1.0.0'],
        'test-',
        ('test-default-v1.0.0', 'default-v1.0.0')
    ),
    (
        ['default-v1.0.0'],
        '',
        ('default-v1.0.0', 'default-v1.0.0')
    ),
])
def test_schema_to_index_with_names(app, index_names, prefix, expected):
    """Test that prefix is added to the index when creating it."""
    new_conf = {'SEARCH_INDEX_PREFIX': prefix}
    with patch.dict(app.config, new_conf):
        result = schema_to_index(
            'default-v1.0.0.json',
            index_names=index_names,
            prefix=prefix
        )

        assert result == expected
