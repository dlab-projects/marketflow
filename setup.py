from setuptools import setup

setup(
    name='marketflow',
    version='0.2.dev0',
    description='Efficiently read TAQ data',
    author='D-Lab Finance Team',
    author_email='davclark@berkeley.edu',
    license='BSD-2',

    packages=['marketflow'],

    install_requires=['tables', 'pytz'],

    extras_require={
        'test': ['pytest', 'arrow'],
    },

    entry_points={
        'console_scripts': [
            'taq2h5=marketflow.hdf5:taq2h5',
            'pyitch=marketflow.ITCHbin:main',
        ],
    },
)
