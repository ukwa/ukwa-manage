python-getvideos
======================

Script for downloading videos and storing them in WARC files. Currently only handles BBC News.

In addition to a 'response' record containing the video it also adds a 'metadata' record containing:

    embedding-page: <URL of the page in which the video was embedded>
    embedding-timestamp: <14-digit timestamp of the time at which the embedding page was rendered>
    element-xpath: <XPath to the element in which the video resides>

