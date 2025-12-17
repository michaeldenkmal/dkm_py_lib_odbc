from setuptools import setup


def read_requirements():
    with open('requirements.txt') as f:
        return f.readlines()


setup(
    name='dkm_py_lib_mssql',
    packages=['dkm_lib_mssql','dkm_lib_orm'],
    description='Denkmal Python SQL-Server',
    version='1.30',
    url='https://github.com/michaeldenkmal/dkm-py-lib-mssql',
    author='michael',
    author_email='michael@denkmal.at',
    keywords=['mssql','denkmal'],
    install_requires=read_requirements()
    )