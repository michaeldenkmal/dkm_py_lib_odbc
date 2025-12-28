from setuptools import setup


def read_requirements():
    with open('requirements.txt') as f:
        return f.readlines()


setup(
    name='dkm_py_lib_odbc',
    packages=['dkm_lib_mssql_odbc','dkm_lib_orm','dkm_lib_odbc'],
    description='Denkmal Python Odbc',
    version='1.0',
    url='https://github.com/michaeldenkmal/dkm-py-lib-odbc',
    author='michael',
    author_email='michael@denkmal.at',
    keywords=['mssql','denkmal','odbc'],
    install_requires=read_requirements()
    )