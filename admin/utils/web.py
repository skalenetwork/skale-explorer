import json
from http import HTTPStatus
from flask import Response


def construct_response(status, data):
    return Response(
        response=json.dumps(data),
        status=status,
        mimetype='application/json'
    )


def construct_ok_response(data=None):
    if data is None:
        data = {}
    return construct_response(HTTPStatus.OK, {'status': 'ok', 'payload': data})