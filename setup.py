from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='shepherd',
    version='1.0.0',
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,

    license='Apache 2.0',
    long_description=open('README.md').read(),
    entry_points={
        'console_scripts': [
            'get-ids-from-hdfs=crawl.sip.ids:main',
            'create-sip=crawl.sip.creator:main',
            'movetohdfs=crawl.hdfs.movetohdfs:main',
            'h3cc=crawl.h3.h3cc:main',
            'w3act=crawl.w3act.w3act_cli:main',
            'pulse=tasks.pulse:main'
        ],
    }
)
