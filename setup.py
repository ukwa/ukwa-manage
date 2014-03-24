from distutils.core import setup

setup(
	name="python-hdfslogs",
	version="0.0.1",
	packages=[ "hdfssync" ],
	author="Roger G. Coram",
	author_email="roger.coram@bl.uk",
	description="HDFS syncing.",
	long_description=open( "README.md" ).read(),
	install_requires=[
		"python-webhdfs",
	],
	data_files=[
		( "/usr/local/bin", [ "hdfslogsync.py" ] ),
		( "/usr/local/bin", [ "hdfscdxsync.py" ] ),
	],
)
