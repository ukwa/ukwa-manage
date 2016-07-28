from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='shepherd',
    version='1.0.0',
    packages=['crawl', 'crawl.h3', 'crawl.hdfs', 'crawl.job', 'crawl.profiles', 'crawl.sip', 'crawl.w3act'],
    install_requires=requirements,
    license='Apache 2.0',
    long_description=open('README.md').read(),
    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.xml', '*.cfg'],
    },
    entry_points={
        'console_scripts': [
            'get-ids-from-hdfs=crawl.sip.ids:main',
            'create-sip=crawl.sip.creator:main',
            'movetohdfs=crawl.hdfs.movetohdfs:main',
            'h3cc=crawl.h3.h3cc.main',
            'w3act=crawl.w3act.w3act_cli.main',
            'pulse=crawl.cli.main'
        ],
    }
)
