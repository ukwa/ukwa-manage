# python-webhdfs
Python wrapper around Hadoop's WebHDFS interface.

    import webhdfs
    w = webhdfs.API(prefix="http://dls.httpfs.wa.bl.uk:14000/webhdfs/v1", user="webhdfs")

#### list(path)
Returns the output of `LISTSTATUS` on `path`.

#### find(path, name="*")
A `generator` which, much like Linux's `find`, yields files matching `name`.

#### open(path)
Returns the contents of `path`. Raises a `TypeError` if `path` is a directory.

#### openstream(path)
Returns the contents of `path` but avoids reading the content into memory for large objects. Raises a `TypeError` if `path` is a directory.

#### exists(path)
Returns a boolean indicating whether `path` exists in HDFS.

#### isdir(path)
Returns a boolean indicating whether `path` is a directory.

#### create(path, file=None, data=None)
Creates a file in HDFS at `path` using either the contents of `file` or raw `data`. Raises an `IOError` if `path` already exists.

#### delete(path, recursive=False)
Deletes a file in HDFS at `path`. Optionally, if `recursive` is `True`, will delete all content beneath it in the hierarchy. Raises an `IOError` if `path` does not exist.

#### checksum(path)
Returns the HDFS checksum for `path`.

#### getmerge(path, output=sys.stdout)
Copies the contents of `path` to `output`. If `path` is a single file, this is the same as `hadoop fs -get`. If `path` is a directory, this is the same as `hadoop fs -getmerge`.

#### readlines(path, decompress=False)
A `generator` which yields individual lines from `path`. If `decompress` is `True`, files are decompressed en route. As with `getmerge()`, `path` can be a single file or a directory.

