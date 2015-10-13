from setuptools import setup

setup(
    name='taq',
    version='0.1.dev',
    description='Efficiently read TAQ data',
    author='D-Lab Finance Team',
    author_email='davclark@berkeley.edu',
    license='BSD-2',

    install_requires=['tables', 'pytz'],

    extras_require={
        'test': ['pytest'],
    },

    entry_points={
        'console_scripts': [
            'pytaq=raw_taq:main',
        ],
    },
)
