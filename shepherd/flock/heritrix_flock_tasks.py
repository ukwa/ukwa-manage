import luigi
import docker


class StartHeritrix(luigi.Task):
    '''
    Initial sketch of how we might spin up many H3 instances via Docker.

    See <https://docker-py.readthedocs.io/en/stable/api/> for the Python API

    NOT in production. Just an idea right now.
    '''
    task_namespace = 'flock'
    job = luigi.Parameter()

    #def requires(self):
    #    return [ StopJob(self.job), CrawlFeed(frequency=self.job.name), CrawlFeed(frequency='nevercrawl') ]

    # Do no output anything, as we don't want anything to prevent restarts, or initiate downstream actions.
    #def output(self):
    #    return luigi.LocalTarget('state/jobs/{}.started.{}'.format(self.job.name, self.date.isoformat()))

    # Always allow re-starting:
    def complete(self):
        return False

    def run(self):
        cli = docker.Client(base_url='unix://var/run/docker.sock')
        instance_name = "h3-%s" % self.job
        print(cli.containers(filters={'name': instance_name}))
        # Stop the old one if it's running:
        if cli.containers(filters={'name': instance_name}):
            cli.stop(instance_name)
        # Remove the old one if it's there:
        if cli.containers(all=True, filters={'name': instance_name}):
            cli.remove_container(instance_name)
        # Create the new one:
        h = cli.create_container('pulse_ukwa-heritrix-lbs', name=instance_name)
        cli.connect_container_to_network(h, "pulse_default", links=
        [ ("clamd","clamd"), ("amqp","amqp"), ("webrender","webrender"), ("acid.matkelly.com","acid.matkelly.com"), ("crawl-test-site","crawl-test-site") ])
        print("Starting %s" % instance_name)
        cli.start(h)
        print(h)
        return

#container_id = cli.create_container(
#    'busybox', 'ls', volumes=['/mnt/vol1', '/mnt/vol2'],
#    host_config=cli.create_host_config(binds=[
#        '/home/user1/:/mnt/vol2',
#        '/var/www:/mnt/vol1:ro',
#    ])
#)

if __name__ == '__main__':
    luigi.run(['flock.StartHeritrix', '--job', 'daily', '--local-scheduler'])
