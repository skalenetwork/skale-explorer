import json
from http import HTTPStatus
from flask import Response


def construct_response(status, data, pretty=False):
    response_data = json.dumps(data, indent=4, sort_keys=True) if pretty else json.dumps(data)
    return Response(
        response=response_data,
        status=status,
        mimetype='application/json'
    )


def construct_ok_response(data=None, pretty=False):
    if data is None:
        data = {}
    return construct_response(HTTPStatus.OK, {'status': 'ok', 'payload': data}, pretty)
