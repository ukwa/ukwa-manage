from distutils.core import setup

setup(
	name="python-sip-verification",
	version="0.0.2",
	description="SIP verification.",
	long_description=open("README.md").read(),
    url="http://git.wa.bl.uk/products/python-sip-verification",
	author="Roger G. Coram",
	author_email="roger.coram@bl.uk",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
    ]
    packages=["sipverify"],
	install_requires=[
		"pika",
		"python-webhdfs",
		"requests",
		"lxml",
		"python-dateutil",
	],
	data_files=[
		("/usr/local/bin", ["verify-sips.py"]),
	],
)
