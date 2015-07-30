python-getvideos
======================

Script for storing videos in WARC files. HTTP URLs will be downloaded, RTMP URLs must have a corresponding file to be read.

    usage: getvideos.py [-h] [-m MULTIPLE] [-p PAGE] [-t TIMESTAMP] [-x XPATH]
                        [-u URL] [-f FILENAME] [-l PLAYLIST] [-y]

    optional arguments:
      -h, --help    show this help message and exit
      -m MULTIPLE   Multiple, comma-separated timestamp/page values.
      -p PAGE       Embedding page.
      -t TIMESTAMP  Embedding page timestamp.
      -x XPATH      XPath to element.
      -u URL        Video URL.
      -f FILENAME   Filename on disk.
      -l PLAYLIST   Playlist of videos.
      -y            YouTube videos [iframes only].

In addition to a 'response' record containing the video it also adds a 'metadata' record containing:

    embedded-video: <URL of the video>
    embedding-timestamp: <14-digit timestamp of the time at which the embedding page was rendered>
    embedded-video-xpath: <XPath to the element in which the video resides>

The metadata's WARC-Target-URI references the embedding page as this makes more sense for future access.

