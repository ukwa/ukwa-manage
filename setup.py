import subprocess
from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

def get_version():
    try:
        return subprocess.check_output(['git', 'describe', '--tags', '--always']).strip()
    except:
        return "?.?.?"

setup(
    name='ukwa-manage',
    version=get_version(),
    packages=['shepherd'],
    #install_requires=requirements, --Seems not to work well with remotes
    include_package_data=True,

    license='Apache 2.0',
    long_description=open('README.md').read(),
    entry_points={
        'console_scripts': [
            'get-ids-from-hdfs=shepherd.lib.sip.ids:main',
            'create-sip=shepherd.lib.lib.sip.creator:main',
            'movetohdfs=crawl.hdfs.movetohdfs:main',
            'inject=shepherd.scripts.inject:main',
            'w3act=shepherd.lib.w3act.w3act_cli:main',
            'pulse=shepherd.tasks.pulse:main',
            'generate-luigi-config=shepherd.lib.tasks.generate_config:main'
        ],
    }
)
