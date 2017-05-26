# -*- coding: utf-8; -*-

"""Test suite for Scheduler."""
import json
import pytest
from unittest import mock

from jobless.commands import command_registry

from tests import base


class TestCommands(base.TestCase):

    @mock.patch('requests.request')
    def test_successful_request(self, request):
        """Tests that a successful response is returned correctly."""
        request.return_value = mock.MagicMock()
        request.return_value.status_code = 200
        request.return_value.text = 'foo'
        args = {'url': 'http://foo.bar', 'method': 'get'}
        success, res = command_registry['http_request'](**args)
        res = json.loads(res)

        request.assert_called_with(**args)
        assert success
        assert res['status_code'] == request.return_value.status_code
        assert res['body'] == request.return_value.text

    @mock.patch('requests.request')
    def test_unsuccessful_request(self, request):
        """Tests that an unsuccessful response is returned correctly."""
        request.return_value = mock.MagicMock()
        request.return_value.status_code = 400
        request.return_value.text = 'bad_request'
        args = {'url': 'http://foo.bar', 'method': 'post'}
        success, res = command_registry['http_request'](**args)
        res = json.loads(res)

        request.assert_called_with(**args)
        assert success is False
        assert res['status_code'] == request.return_value.status_code
        assert res['body'] == request.return_value.text

    def test_failed_request(self):
        success, res = command_registry['http_request']()
        res = json.loads(res)
        assert success is False
        assert res['status_code'] is None
        assert res['body'] == 'request() missing 2 required positional arguments: ' \
                              '\'method\' and \'url\''
