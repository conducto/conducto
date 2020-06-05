import http
import json
import re
import urllib
import urllib.request
from urllib.parse import urlparse


def _add_status_code(response):
    if isinstance(response.status, int):
        code = response.status
    else:
        try:
            code = int(re.search(r"^(\d\d\d)", response.status).groups()[0])
        except:
            response.status_code = response.status
            return
    response.status_code = http.HTTPStatus(code)


def _put_params(url, params):
    if params is None:
        return url
    params_enc = urllib.parse.urlencode(params)
    url_parts = list(urllib.parse.urlparse(url))
    url_parts[4] = params_enc
    return urllib.parse.urlunparse(url_parts)


def _get_json_bytes(data):
    if data is None:
        data = {}
    if isinstance(data, dict):
        data = json.dumps(data)
    if isinstance(data, str):
        data = data.encode("utf8")
    return data


def parse(url):
    return urlparse(url)


def get(url, headers=None, params=None):
    if headers is None:
        headers = {}
    url = _put_params(url, params)
    the_request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        response = urllib.request.urlopen(the_request)
    except urllib.error.HTTPError as e:
        response = e
        response.url = url
    _add_status_code(response)
    return response


def head(url, headers=None, params=None):
    if headers is None:
        headers = {}
    url = _put_params(url, params)
    the_request = urllib.request.Request(url, headers=headers, method="HEAD")
    try:
        response = urllib.request.urlopen(the_request)
    except urllib.error.HTTPError as e:
        response = e
        response.url = url
    _add_status_code(response)
    return response


def put(url, headers=None, data=None):
    if headers is None:
        headers = {}
    data = _get_json_bytes(data)
    assert isinstance(data, bytes), f"data is of type {type(data)}."
    the_request = urllib.request.Request(url, headers=headers, data=data, method="PUT")
    try:
        response = urllib.request.urlopen(the_request)
    except urllib.error.HTTPError as e:
        response = e
        response.url = url
    _add_status_code(response)
    return response


def post(url, headers=None, data=None):
    if headers is None:
        headers = {}
    data = _get_json_bytes(data)
    assert isinstance(data, bytes), f"data is of type {type(data)}."
    the_request = urllib.request.Request(url, headers=headers, data=data, method="POST")
    try:
        response = urllib.request.urlopen(the_request)
    except urllib.error.HTTPError as e:
        response = e
        response.url = url
    _add_status_code(response)
    return response


def delete(url, headers=None, params=None):
    if headers is None:
        headers = {}
    url = _put_params(url, params)
    the_request = urllib.request.Request(url, headers=headers, method="DELETE")
    try:
        response = urllib.request.urlopen(the_request)
    except urllib.error.HTTPError as e:
        response = e
        response.url = url
    _add_status_code(response)
    return response


def patch(url, headers=None, data=None):
    if headers is None:
        headers = {}
    data = _get_json_bytes(data)
    assert isinstance(data, bytes), f"data is of type {type(data)}."
    the_request = urllib.request.Request(
        url, headers=headers, data=data, method="PATCH"
    )
    try:
        response = urllib.request.urlopen(the_request)
    except urllib.error.HTTPError as e:
        response = e
        response.url = url
    _add_status_code(response)
    return response
