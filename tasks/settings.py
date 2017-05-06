import os
import luigi

class state(luigi.Config):
    folder = os.environ.get('LUIGI_STATE_FOLDER', luigi.Parameter(default='./state'))
    hdfs_folder = os.environ.get('HDFS_STATE_FOLDER', luigi.Parameter(default='state'))


class act(luigi.Config):
    url = os.environ.get('W3ACT_URL', luigi.Parameter(default='http://w3act:9000/act'))
    username = os.environ.get('W3ACT_USER', luigi.Parameter(default='wa-sysadm@bl.uk'))
    password = os.environ.get('W3ACT_PW', luigi.Parameter(default='sysAdmin'))


class h3(luigi.Config):
    host = luigi.Parameter(default='ukwa-heritrix')
    port = luigi.IntParameter(default=8443)
    username = os.environ.get('HERITRIX_USER', luigi.Parameter(default='heritrix'))
    password = os.environ.get('HERITRIX_PASSWORD', luigi.Parameter(default='heritrix'))
    local_root_folder = luigi.Parameter(default='/heritrix')
    local_job_folder = luigi.Parameter(default='/jobs')
    local_wren_folder = luigi.Parameter(default='/heritrix/wren')
    hdfs_prefix = os.environ.get('HDFS_PREFIX', luigi.Parameter(''))
    local_prefix = os.environ.get('LOCAL_PREFIX', luigi.Parameter(''))


# WARC_ROOT = "%s/output/warcs" % h3().local_root_folder
# VIRAL_ROOT = "%s/output/viral" % h3().local_root_folder
# IMAGE_ROOT = "%s/output/images" % h3().local_root_folder
# LOG_ROOT = "%s/output/logs" % h3().local_root_folder
# LOCAL_LOG_ROOT = "%s/output/logs" % h3().local_root_folder


class systems(luigi.Config):
    cdxserver = os.environ.get('CDXSERVER_URL', luigi.Parameter(default='http://cdxserver:8080/fc'))
    wayback = os.environ.get('WAYBACK_URL', luigi.Parameter(default='http://openwayback:8080/wayback'))
    wrender = os.environ.get('WRENDER_URL', luigi.Parameter(default='http://webrender:8010/render'))
    # Prefix for webhdfs queries, separate from general Luigi HDFS configuration.
    # e.g. http://localhost:50070/webhdfs/v1
    webhdfs = os.environ.get('WEBHDFS_PREFIX', luigi.Parameter(default='http://hadoop:50070/webhdfs/v1'))
    amqp_host = os.environ.get('AMQP_HOST', luigi.Parameter(default='amqp'))
    clamd_host = os.environ.get('CLAMD_HOST', luigi.Parameter(default='clamd'))
    clamd_port = os.environ.get('CLAMD_PORT', luigi.IntParameter(default=3310))
    elasticsearch_host = os.environ.get('ELASTICSEARCH_HOST', luigi.Parameter(default='monitrix'))
    elasticsearch_port = os.environ.get('ELASTICSEARCH_PORT', luigi.IntParameter(default=9200))
    elasticsearch_index_prefix = os.environ.get('ELASTICSEARCH_INDEX_PREFIX', luigi.Parameter(default='pulse'))
    servers = luigi.Parameter(default='/shepherd/tasks/servers.json')
    services = luigi.Parameter(default='/shepherd/tasks/services.json')


class slack(luigi.Config):
    token = os.environ.get('SLACK_TOKEN', luigi.Parameter())

