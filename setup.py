import subprocess
from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

def get_version():
    try:
        return subprocess.check_output(['git', 'describe', '--tags', '--always']).strip().decode("utf-8")
    except:
        return "?.?.?"

setup(
    name='ukwa-manage',
    version=get_version(),
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    license='Apache 2.0',
    long_description=open('README.md').read(),
    entry_points={
        'console_scripts': [
            'trackdb=lib.trackdb.cmd:main',
            'windex=lib.windex.cmd:main',
            'store=lib.store.cmd:main',
            'docharv=lib.docharvester.cmd:main',
            'tidy-logs=lib.store.tidy_logs:main',
            'warctidy=lib.store.warc_tidy:main'
        ]
    }
)
