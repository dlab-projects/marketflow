from setuptools import setup

setup(
    name='taq',
    version='0.1.dev',
    description='Efficiently read TAQ data',
    author='D-Lab Finance Team',
    author_email='davclark@berkeley.edu',
    license='BSD-2',

    packages=['taq'],

    install_requires=['tables', 'pytz'],

    extras_require={
        'test': ['pytest', 'arrow'],
    },

    entry_points={
        'console_scripts': [
            'pytaq=taq.raw_taq:main',
            'pyitch=taq.ITCHbin:main',
        ],
    },
)
