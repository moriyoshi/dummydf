import os
import sys
from setuptools import setup, find_packages

about = {}
with open(os.path.join(os.path.dirname(__file__), 'dummydf', '__version__.py')) as f:
    exec(f.read(), about)


setup(
    name=about['__title__'],
    version=about['__version__'],
    description=about['__description__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    maintainer=about['__maintainer__'],
    maintainer_email=about['__maintainer_email__'],
    license=about['__license__'],
    url=about['__url__'],
    zip_safe=False,
    packages=['dummydf'],
    install_requires=[
        'pandas >= 0.21.0',
        'six',
    ],
    setup_requires=[
        'pytest-runner',
    ],
    extras_require={
        'codecov': [
            'codecov',
        ],
    },
    tests_require=[
        'pytest >= 3.0.0',
        'pytest-cov >= 2.0.0',
    ]
)
