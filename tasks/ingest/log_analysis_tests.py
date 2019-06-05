import json
import luigi
from tasks.ingest.log_analysis_hadoop import SummariseLogFiles, ListDeadSeeds, CountStatusCodes


def test_run_summariser():
    task = SummariseLogFiles( [ '../../test/fragment-of-a-crawl.log' ], 'dc','20170220090024', False )
    luigi.build([task], local_scheduler=True)

    count = 0
    with task.output().open() as f:
        for line in f.readlines():
            key, val = line.split('\t',1)
            jl = json.loads(val)
            print(key, jl)
            count = count + 1

    assert count is 6


def test_run_dead_seeds_list():
    task = ListDeadSeeds( [ '../../test/fragment-of-a-crawl-with-dead-seeds.log' ], 'dc','20170220090024', False )
    luigi.build([task], local_scheduler=True)

    count = 0
    with task.output().open() as f:
        for line in f.readlines():
            key, val = line.split('\t',1)
            print(key, val)
            count = count + 1

    assert count is 2

    
def test_run_count_status_codes():
    task = CountStatusCodes( [ '../../test/fragment-of-a-crawl.log' ], 'dc','20170220090024', False )
    luigi.build([task], local_scheduler=True)

    count = 0
    with task.output().open() as f:
        for line in f.readlines():
            key, val = line.split('\t',1)
            print(key, val)
            count = count + 1

    assert count is 6
