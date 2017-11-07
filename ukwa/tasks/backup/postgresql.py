import os
import luigi
import luigi.contrib.ssh
import luigi.contrib.hdfs
from luigi.contrib.hdfs import PlainFormat
import datetime


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

        # Launch pg_dump remotely
        rc = luigi.contrib.ssh.RemoteContext(self.host)
        cmd = ['docker', 'exec', self.service,
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

    host = luigi.Parameter(default='crawler01')
    service = luigi.Parameter(default='pulsefeprod_postgres_1')
    db = luigi.Parameter(default='w3act')
    remote_host_backup_folder = luigi.Parameter(default='/data/prod/postgresql')
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
        with self.input().open('rb') as reader:
            with self.output().open('w') as writer:
                for chunk in reader:
                    writer.write(chunk)

    def get_backup_size(self):
        return self.output().fs.count(self.output().path)
        # WebHDFS
        #return self.output().fs.client.status(self.output().path).get('length',None)


@BackupProductionW3ACTPostgres.event_handler(luigi.Event.SUCCESS)
def update_task_metrics(task):
    # type: (BackupProductionW3ACTPostgres) -> None
    """
       Will be called directly after a successful execution
       and is used to update any relevant metrics
    """
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    registry = CollectorRegistry()
    g = Gauge('ukwa_task_backup_w3act_pgdb_timestamp', 'Last time the W3ACT database was backed up', registry=registry)
    g.set_to_current_time()

    g2 = Gauge('ukwa_task_backup_w3act_pgdb_size', 'Size of the W3ACT database backup', registry=registry)
    g2.set(task.get_backup_size())

    push_to_gateway('dev-monitor.n45.wa.bl.uk:9091', job=task.get_task_family(), registry=registry)


if __name__ == '__main__':
    #luigi.run(['backup.BackupProductionW3ACTPostgres'])

    task = BackupProductionW3ACTPostgres()
    update_task_metrics(task)
