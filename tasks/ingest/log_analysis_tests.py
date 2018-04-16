import json
import luigi
from tasks.ingest.log_analysis_hadoop import SummariseLogFiles


def test_run_summariser():
    task = SummariseLogFiles( [ 'test/logs/fragment-of-a-crawl.log' ], 'dc','20170220090024', False )
    luigi.build([task], local_scheduler=True)

    count = 0
    with task.output().open() as f:
        for line in f.readlines():
            key, val = line.split('\t',1)
            jl = json.loads(val)
            print(key, jl)
            count = count + 1

    assert count is 6
