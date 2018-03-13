# Overall purpose here is to import data from third parties

import os
import luigi
import datetime
import luigi.contrib.ftp
import luigi.contrib.hdfs
import lib.webhdfs

#: the FTP server
NOM_HOST = os.environ['NOM_HOST']
#: the username
NOM_USER = os.environ['NOM_USER']
#: the password
NOM_PWD = os.environ['NOM_PWD']

# 1MB chunks
DEFAULT_BUFFER_SIZE = 1024*1000


class NominetDomainListFTP(luigi.ExternalTask):
    """
    Remote SFTP service and filenaming pattern for monthly releases.

    NOTE that for this to work, the host key must be set up and known to the server that runs this task. e.g.
    a `ssh USER@HOST` check to get the key registered will be needed to set up a new server or if the remote server changes.

    """
    date = luigi.MonthParameter(default=datetime.date.today())

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file that will be created in a FTP server.
        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        filename = '/home/bl/domains.%s.csv.gz' % self.date.strftime('%Y%m')
        return luigi.contrib.ftp.RemoteTarget(filename, NOM_HOST, username=NOM_USER, password=NOM_PWD, sftp=True)


class NominetDomainListToHDFS(luigi.Task):
    """
    """
    date = luigi.MonthParameter(default=datetime.date.today())

    def requires(self):
        return NominetDomainListFTP(date=self.date)

    def output(self):
        filename = "/1_data/nominet/domains.%s.csv.gz" % self.date.strftime('%Y%m')
        return luigi.contrib.hdfs.HdfsTarget(path=filename, format=lib.webhdfs.WebHdfsPlainFormat)

    def run(self):
        # Read the file in and write it to HDFS
        with self.input().open() as reader:
            with self.output().open('w') as writer:
                while True:
                    chunk = reader.read(DEFAULT_BUFFER_SIZE)
                    if not chunk:
                        break
                    writer.write(chunk)


if __name__ == '__main__':
    luigi.run(['NominetDomainListToHDFS'])
