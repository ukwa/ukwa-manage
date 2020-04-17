windex
=======

The web-archive index tool.

We can query the CDX from the command-line:

```
$ windex -C ethos cdx-query http://theses.gla.ac.uk/1158/1/2009ibrahamphd.pdf
uk,ac,gla,theses)/1158/1/2009ibrahamphd.pdf 20200404014648 http://theses.gla.ac.uk/1158/1/2009ibrahamphd.pdf application/pdf 200 FH7MXPURQT7S75IVEUUFWPA2XPOTY3VW - - 7803924 643334769 /1_data/ethos/warcs/WARCPROX-20200404014942362-00230-mja43xl7.warc.gz
```

Now we use the filename, offset and length to grab the WARC record:

```
$ store get --offset 643334769 --length 7803924 /1_data/ethos/warcs/WARCPROX-20200404014942362-00230-mja43xl7.warc.gz temp.warc.gz
```

This gets the WARC record (and oddly, all following ones?!)

```
$ warcio extract --payload temp.warc.gz 0 > file.pdf
```

So now we have the PDF.
