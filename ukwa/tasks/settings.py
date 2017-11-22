import os
import luigi


class state(luigi.Config):
    folder = luigi.Parameter(default='/var/task-state')
    hdfs_folder = luigi.Parameter(default='/9_processing/task-state/')


class act(luigi.Config):
    url = luigi.Parameter(default='http://w3act:9000/act')
    username = luigi.Parameter(default='wa-sysadm@bl.uk')
    password = luigi.Parameter(default='sysAdmin')


class h3(luigi.Config):
    host = luigi.Parameter(default='ukwa-heritrix')
    port = luigi.IntParameter(default=8443)
    username = luigi.Parameter(default='heritrix')
    password = luigi.Parameter(default='heritrix')
    local_root_folder = luigi.Parameter(default='/heritrix')
    local_job_folder = luigi.Parameter(default='/heritrix/jobs')
    local_wren_folder = luigi.Parameter(default='/heritrix/wren')
    hdfs_prefix = luigi.Parameter(default='')
    local_prefix = luigi.Parameter(default='')


# WARC_ROOT = "%s/output/warcs" % h3().local_root_folder
# VIRAL_ROOT = "%s/output/viral" % h3().local_root_folder
# IMAGE_ROOT = "%s/output/images" % h3().local_root_folder
# LOG_ROOT = "%s/output/logs" % h3().local_root_folder
# LOCAL_LOG_ROOT = "%s/output/logs" % h3().local_root_folder


class systems(luigi.Config):
    # Prefix for webhdfs queries, separate from general Luigi HDFS configuration.
    # e.g. http://localhost:50070/webhdfs/v1
    webhdfs = luigi.Parameter(default='http://hadoop:50070/webhdfs/v1')
    webhdfs_user = luigi.Parameter(default='hdfs')
    # Other UKWA systems:
    cdxserver = luigi.Parameter(default='http://cdxserver:8080/fc')
    wayback = luigi.Parameter(default='http://openwayback:8080/wayback')
    wrender = luigi.Parameter(default='http://webrender:8010/render')
    # For metrics:
    prometheus_push_gateway = luigi.Parameter(default='dev-monitor.n45.wa.bl.uk:9091')
    # For tracking some task outputs
    elasticsearch_host = luigi.Parameter(default='')
    elasticsearch_port = luigi.IntParameter(default=9200)
    elasticsearch_index_prefix = luigi.Parameter(default='ukwa-manage-tasks')
