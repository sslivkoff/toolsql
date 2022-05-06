import setuptools


setuptools.setup(
    name='toolsql',
    version='0.2.4',
    packages=setuptools.find_packages(),
    install_requires=[
        'alembic',
        'sqlalchemy',
        'toolcache',
        'toolcli',
        'typing_extensions',
    ],
)

