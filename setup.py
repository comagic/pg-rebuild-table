import sys

from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand

REPO_GROUP = 'pg-tools'
REPO_NAME = 'pg-rebuild-table'
PACKAGE_DESC = f'{REPO_GROUP} {REPO_NAME}'
PACKAGE_LONG_DESC = 'The script rebuilds the table'
PACKAGE_VERSION = '0.1.2'


# Используем ручной запуск с помощью класса PyTest
class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        # default list of options for testing
        # https://docs.pytest.org/en/latest/logging.html
        self.pytest_args = (
            '--flake8 {0} tests '
            '--junitxml=.reports/{0}_junit.xml '
            '--cov={0} --cov=tests '
            '-p no:logging'.format(REPO_NAME.replace('-', '_'))
        )

    def run_tests(self):
        import shlex
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(shlex.split(self.pytest_args))
        sys.exit(errno)


# Что нужно для запуска python setup.py <any_cmd>
setup_requires = []

# Что нужно для установки
install_requires = [
    'asyncpg',
    'munch',
]

# Опции (pip install <package>[option])
extras_require = {
}

# Что нужно для запуска python setup.py test
tests_require = [
    'flake8==3.8.3',
    'pytest==5.4.3',
    'pytest-cov==2.9.0',
    'pytest-flake8==1.0.6',
    'pytest-asyncio==0.12.0',
    'asynctest==0.13.0',
]

# Скрипты
console_scripts = [
    'pg_rebuild_table=pg_rebuild_table.main:main'
]

setup(
    name=f'{REPO_NAME}'.replace('-', '_'),
    version=PACKAGE_VERSION,
    description=PACKAGE_DESC,
    long_description=PACKAGE_LONG_DESC,
    url=f'https://gitlab.uis.dev/{REPO_GROUP}/{REPO_NAME}',
    author="Viktor Vasilev",
    author_email="v.vasilev@comagic.dev",
    license="Nodefined",
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Framework :: AsyncIO',
        'Framework :: Aioamqp',
        'Framework :: Pytest',
        'Intended Audience :: Information Technology',
        'License :: Other/Proprietary License',
        'License :: UIS License',
        'Natural Language :: Russian',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.7',
        'Topic :: UIS:: Microservices',
    ],
    zip_safe=False,
    packages=find_packages(exclude=['tests', 'examples', '.reports']),
    package_data={'': ['*.lua']},
    package_dir={'pg_rebuild_table': 'pg_rebuild_table'},
    include_package_data=True,
    entry_points={'console_scripts': console_scripts},
    python_requires='>=3.7',
    setup_requires=setup_requires,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require=extras_require,
    cmdclass={'test': PyTest},
)
