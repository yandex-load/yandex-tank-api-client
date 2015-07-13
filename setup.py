#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='yandex-tank-api-client',
    version='0.0.3',
    description='yandex-tank-api client python module',
    long_description="""
        Classes for working with yandex-tank-api.""",
    packages=['yandex_tank_api_client'],
    install_requires=['trollius'],
    scripts=[],
)
