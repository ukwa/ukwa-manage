import luigi.contrib.ssh


if __name__ == '__main__':
    rfs = luigi.contrib.ssh.RemoteFileSystem("crawler04.bl.uk", username="root", key_file="ssh-id/id_rsa")
    for f in rfs.listdir("/heritrix/output/warcs/dc3-20160810"):
        print(f)