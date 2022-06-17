import functools
import traceback

from flask import jsonify
from flask import request


class RouteBuilder:
    __slots__ = (
        "_expected_args"
    )

    def __init__(self, expected_args: set = set([])):
        self._expected_args = expected_args

    def __call__(self, func):

        @functools.wraps(func)
        def wrapper_route(*args, **kwargs):
            try:
                assert request.args.keys() == self._expected_args, f"{self._expected_args} args expected!"

                return jsonify(func(*args, **kwargs))

            except (Exception,):

                return jsonify(exception=traceback.format_exc()), 400

        return wrapper_route
