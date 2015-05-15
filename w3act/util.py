"""Utility classes/methods."""

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

