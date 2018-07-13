import sys
import json

print("\n".join(sys.argv))

sources = [ '/Users/andy/Dropbox/Work/WAP/Data/DC/kafka-report-2018-07-13-11-56.json',
            '/Users/andy/Dropbox/Work/WAP/Data/DC/kafka-report-2018-07-13-12-15.json',
            '/Users/andy/Dropbox/Work/WAP/Data/DC/kafka-report-2018-07-13-13-12.json' ]

for source in sources:
    paritions = {}
    consumed = 0
    with open(source) as f:
        print(source)
        for h in json.load(f):
            kr = h['state']['message']
            counter = 0
            for line in kr.split('\n'):
                if "partition: " in line:
                    line = line.replace("  partition: ","")
                    line = line.replace(" offset: ","")
                    p, o = line.split(',')
                    p = int(p)
                    o = int(o)
                    if p in paritions:
                        print("ACK")
                        exit()
                    paritions[p] = o
                    consumed += o
                    counter += 1
            print("Count: %i" % counter)

    print(json.dumps(paritions, indent=2))
    print("Total Consumed: %i" % consumed)
