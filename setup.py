from distutils.core import setup

setup(
	name="python-webhdfs",
	version="0.1.0",
	author="PsypherPunk",
	author_email="psypherpunk@gmail.com",
	packages=[ "webhdfs" ],
	license="LICENSE.txt",
	description="WebHDFS wrapper.",
	long_description=open( "README.md" ).read(),
	install_requires=[
		"requests >= 2.0.0",
	],
)
