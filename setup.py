from distutils.core import setup

setup(
    name="python-w3act-now",
    version="0.0.1",
    author="Roger G. Coram",
    author_email="roger.coram@bl.uk",
    packages=["w3act"],
    license="LICENSE.txt",
    description="Heritrix/w3act job launcher.",
    long_description=open("README.md").read(),
    install_requires=[
        "python-heritrix",
        "requests",
    ],
    data_files=[
        ("/etc/init.d", ["bin/w3actd"]),
        ("/usr/local/bin", ["w3actdaemon.py"]),
    ]
)
