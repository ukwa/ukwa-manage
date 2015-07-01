"""Utility classes/methods."""
import re
import operator
from collections import Counter, OrderedDict

def human_readable(bytes, precision=1):
    abbrevs = (
        (1<<50L, "PB"),
        (1<<40L, "TB"),
        (1<<30L, "GB"),
        (1<<20L, "MB"),
        (1<<10L, "kB"),
        (1, "bytes")
    )
    if bytes == 1:
        return "1 byte"
    for factor, suffix in abbrevs:
        if bytes >= factor:
            break
    return "%.*f %s" % (precision, bytes / factor, suffix)


class dotdict(dict):
    """dot.notation access to dictionary"""
    def __getattr__(self, attr):
        return self.get(attr)
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__


def generate_log_stats(logs):
    response_codes = []
    data_size = 0
    host_regex = re.compile("https?://([^/]+)/.*$")
    all_hosts_data = {}
    for log in logs:
        with open(log, "rb") as l:
            for line in l:
                # Annotations can contain whitespace so limit the split.
                fields = line.split(None, 15)
                match = host_regex.match(fields[3])
                if match is not None:
                    host = match.group(1)
                    try:
                        host_data = all_hosts_data[host]
                    except KeyError:
                        all_hosts_data[host] = { "data_size": 0, "response_codes": [] }
                        host_data = all_hosts_data[host]
                    host_data["response_codes"].append(fields[1])
                    if fields[2] != "-":
                        host_data["data_size"] += int(fields[2])
                    if "serverMaxSuccessKb" in fields[-1]:
                        host_data["reached_cap"] = True
    all_hosts_data = OrderedDict(sorted(all_hosts_data.iteritems(), key=operator.itemgetter(1), reverse=True))
    for host, data in all_hosts_data.iteritems():
        data["response_codes"] = Counter(data["response_codes"])
        data["data_size"] = human_readable(data["data_size"])
    return all_hosts_data

