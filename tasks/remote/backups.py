import os
import luigi
import luigi.contrib.ssh
import datetime


class BackupRemoteDockerPostgres(luigi.Task):
    """
    """
    task_namespace = 'backup'
    host = luigi.Parameter()
    service = luigi.Parameter()
    remote_host_backup_folder = luigi.Parameter()
    db = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    backup_folder = luigi.Parameter(default='/backups/psql/')

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
                'pg_dump', '--format=c', '--file=%s' % remote_docker_backup_path, self.db ]
        rc.check_output(cmd)

        #cmd = 'su - postgres -c "/usr/bin/pg_dump --format=c --file=%s %s"' % (remote_backup_path, self.db)

        # Copy the resultant file back here:
        with self.output().temporary_path() as temp_path:
            rt.get(temp_path)


if __name__ == '__main__':
    luigi.run(['backup.BackupRemoteDockerPostgres', '--local-scheduler',
                '--host', 'crawler07', '--service', 'pulsedeploy_postgres_1', '--db', 'w3act',
                '--remote-host-backup-folder', '/zfspool/test/postgresql'])
#    luigi.run(['backup.BackupRemoteDockerPostgres',
#                '--host', 'crawler07', '--service', 'pulse_prod_postgres_1' '--db', 'w3act',
#                '--remote-host-backup-folder', '/zfspool/prod/postgresql'])
