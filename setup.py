# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['pybotters_data_collect']

package_data = \
{'': ['*']}

install_requires = \
['loguru>=0.6.0,<0.7.0', 'motor>=3.0.0,<4.0.0', 'pybotters>=0.13.0,<0.14.0']

setup_kwargs = {
    'name': 'pybotters-data-collect',
    'version': '0.1.0',
    'description': '',
    'long_description': None,
    'author': 'ko0hi',
    'author_email': 'ko0hi.4731@gmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.10,<4.0',
}


setup(**setup_kwargs)
