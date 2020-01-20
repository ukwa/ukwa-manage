import os
import luigi
import luigi.contrib.ssh
import luigi.contrib.hdfs
from luigi.contrib.hdfs import PlainFormat
import datetime
from prometheus_client import Gauge, CollectorRegistry


class BackupRemoteDockerPostgres(luigi.Task):
    """
    This task is designed to create a PostgreSQL dump for a database running under Docker,
    and copy it back to the local filesystem.
    """
    task_namespace = 'backup'

    host = luigi.Parameter()
    service = luigi.Parameter()
    remote_host_backup_folder = luigi.Parameter()
    db = luigi.Parameter()
    backup_folder = luigi.Parameter(default='/backups/psql/')
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        path = os.path.join(self.backup_folder,"%s-%s-%s.pgdump-%s" %
                            (self.host, self.service, self.db, self.date.strftime('%Y%m%d')))
        return luigi.LocalTarget(path)

    def run(self):
        # Check the remote DB path and clear it if necessary:
        remote_docker_backup_path = os.path.join( "/var/lib/postgresql/data",
                                           "%s.pgdump-%s" % (self.db, self.date.strftime('%Y%m%d')))
        remote_host_backup_path = os.path.join( self.remote_host_backup_folder,
                                           "%s.pgdump-%s" % (self.db, self.date.strftime('%Y%m%d')))
        rt = luigi.contrib.ssh.RemoteTarget(host=self.host, path=remote_host_backup_path)
        if rt.exists():
            rt.remove()

        # Launch shell remotely
        rc = luigi.contrib.ssh.RemoteContext(self.host)
        
        # get the id of the container - straight service name may not work 
        # e.g. if it's an instance in a swarm: mydbservice (name) > mydbservice.1.idinfo (instance) 
        cmd = ['docker', 'ps', '-q', '-f', 'name=%s' % self.service]
        containerid=rc.check_output(cmd).rstrip()
          
        # execute the pg_dump command against the container  
        cmd = ['docker', 'exec', containerid,
                'pg_dump', '-U', self.db, '--format=c', '--file=%s' % remote_docker_backup_path, self.db ]
        rc.check_output(cmd)

        #cmd = 'su - postgres -c "/usr/bin/pg_dump --format=c --file=%s %s"' % (remote_backup_path, self.db)

        # Copy the resultant file back here:
        with self.output().temporary_path() as temp_path:
            rt.get(temp_path)


class BackupProductionW3ACTPostgres(luigi.Task):
    """
    This task runs a Docker PostgreSQL backup task for a specific system (production W3ACT)
    and then pushes the backup file up to HDFS.
    """
    task_namespace = 'backup'
    host = luigi.Parameter(default='ingest')
    service = luigi.Parameter(default='ife_prod_postgres')
    db = luigi.Parameter(default='w3act')
    remote_host_backup_folder = luigi.Parameter(default='/data/prod/w3act/postgresql')
    hdfs_backup_folder = luigi.Parameter(default='/2_backups/')
    date = luigi.DateParameter(default=datetime.date.today())
    
    def requires(self):
        return BackupRemoteDockerPostgres(host=self.host, service=self.service, db=self.db,
                                          remote_host_backup_folder=self.remote_host_backup_folder,
                                          date=self.date)

    def output(self):
        bkp_path = os.path.join(self.hdfs_backup_folder,"%s/%s/%s.pgdump-%s" %
                            (self.host, self.service, self.db, self.date.strftime('%Y%m%d')))
        return luigi.contrib.hdfs.HdfsTarget(path=bkp_path, format=PlainFormat())

    def run(self):
        with open(self.input().path, 'rb') as reader:
           with self.output().open('w') as writer:
                for chunk in reader:
                    writer.write(chunk)

    def get_backup_size(self):
        return self.output().fs.count(self.output().path).get('content_size', None)
        # WebHDFS
        # return self.output().fs.client.status(self.output().path).get('length',None)

    def get_metrics(self,registry):
        # type: (CollectorRegistry) -> None
        g = Gauge('ukwa_database_backup_size_bytes', 'Size of a database backup.',
                  labelnames=['db'], registry=registry)
        g.labels(db=self.db).set(self.get_backup_size())

        
class BackupProductionShinePostgres(luigi.Task):
    """
    This task runs a Docker PostgreSQL backup task for a specific system (production Shine)
    and then pushes the backup file up to HDFS.
    """
    task_namespace = 'backup'

    host = luigi.Parameter(default='access')
    service = luigi.Parameter(default='access_shinedb')
    db = luigi.Parameter(default='shine')
    
    #the host dir mapped to the container data dir that the dump is in
    remote_host_backup_folder = luigi.Parameter(default='/data/shine/postgresql/data') 
    hdfs_backup_folder = luigi.Parameter(default='/2_backups/')
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return BackupRemoteDockerPostgres(host=self.host, service=self.service, db=self.db,
                                          remote_host_backup_folder=self.remote_host_backup_folder,
                                          date=self.date)

    def output(self):
        bkp_path = os.path.join(self.hdfs_backup_folder,"%s/%s/%s.pgdump-%s" %
                            (self.host, self.service, self.db, self.date.strftime('%Y%m%d')))
        return luigi.contrib.hdfs.HdfsTarget(path=bkp_path, format=PlainFormat())

    def run(self):
        with open(self.input().path, 'rb') as reader:
            with self.output().open('w') as writer:
                for chunk in reader:
                    writer.write(chunk)

    def get_backup_size(self):
        return self.output().fs.count(self.output().path).get('content_size', None)
        # WebHDFS
        # return self.output().fs.client.status(self.output().path).get('length',None)

    def get_metrics(self,registry):
        # type: (CollectorRegistry) -> None
        g = Gauge('ukwa_database_backup_size_bytes', 'Size of a database backup.',
                  labelnames=['db'], registry=registry)
        g.labels(db=self.db).set(self.get_backup_size())

