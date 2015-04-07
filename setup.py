from distutils.core import setup

setup(
    name="python-w3act",
    version="0.0.2",
    author="Roger G. Coram",
    author_email="roger.coram@bl.uk",
    packages=[
        "w3act",
        "w3act.watched",
    ],
    license="LICENSE.txt",
    description="Heritrix/w3act job launcher.",
    long_description=open("README.md").read(),
    install_requires=[
        "python-heritrix",
        "requests",
        "python-dateutil",
        "lxml",
    ],
    data_files=[
        ("/etc/init.d", ["bin/w3actd"]),
        ("/usr/local/bin", ["w3actdaemon.py"]),
        ("/usr/local/bin", ["w3start.py"]),
        ("/usr/local/bin", ["w3add.py"]),
    ]
)
