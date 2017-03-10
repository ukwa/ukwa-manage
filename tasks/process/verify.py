import requests
import hashlib


def get_hash_of_ark(ark):
    if ark.startswith('ark:/81055/'):
        ark = ark[11:]
    url = "http://dls.gtw.wa.bl.uk/%s" % ark
    return get_hash_of_url(url)


def get_hash_of_url(url):
    try:
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            sha = hashlib.sha512()
            for block in r.iter_content(2048):
                sha.update(block)
            return r.status_code, sha.hexdigest()
        else:
            return r.status_code, None
    except Exception as e:
        print(e)
        return None, None


if __name__ == "__main__":
    print(get_hash_of_url("http://anjackson.net"))
    #print(get_hash_of_ark("ark:/81055/vdc_100000038622.0x00784c"))