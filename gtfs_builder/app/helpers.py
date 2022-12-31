import functools
import traceback

from flask import jsonify
from flask import request


class RouteBuilder:
    __slots__ = (
        "_full_expected_args",
        "_expected_args",
        "_optional_args"
    )

    def __init__(self, expected_args=None, optional_args=None):
        if optional_args is None:
            optional_args = set([])

        if expected_args is None:
            expected_args = set([])

        self._expected_args = expected_args
        self._optional_args = optional_args
        self._full_expected_args = set()

    def __call__(self, func):

        @functools.wraps(func)
        def wrapper_route(*args, **kwargs):
            try:
                self._full_expected_args.update(self._expected_args)
                self._full_expected_args.update(self._optional_args)
                assert any([
                    request.args.keys() == self._expected_args, request.args.keys() == self._full_expected_args]
                ), f"{self._expected_args} args expected!"

                return jsonify(func(*args, **kwargs))

            except (Exception,):

                return jsonify(exception=traceback.format_exc()), 400

        return wrapper_route
