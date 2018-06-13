import pandas as pd
import urllib.request
from lxml import etree
import os
import time
import requests
import re
import argparse
from multiprocessing import Pool
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(description="ts crawler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--url', type=str, default='http://ssex.com/tag/asian/', help="remote url to crawl")
    parser.add_argument('--forward',default=5,type=int,help="download number of ts ahead")
    parser.add_argument('--down_rooms',default=100,type=int,help="download number of rooms")  
    parser.add_argument('--cpus',default=10,type=int,help="num of process")
    parser.add_argument('--bucket',type=str,help="qiniu bucket if not set will download ts to local")                        
    args = parser.parse_args()
    return args
    

args = parse_args()

def download_ts(cmd, bucket=None, qiniu_path=''):
    if bucket is None:
        os.system(cmd)
    elif qiniu_path != '':
        tmp = cmd.split(' ')
        url = tmp[1]
        os.system("qshell account elYxU2yfITGA45M2dL1up4k9IYc1ehgRWzrh22Sk PxfV1B2Mfy4NuT0OuDdnjGIe7V478kJ6v8wxwUy_")
        cmd = 'qshell fetch ' + url + ' ' + bucket + ' ' + qiniu_path
        os.system(cmd)
        

def main(hrefs, bucket=None, output_dir='./', forward=5, down_list=100):
    for href in hrefs:
        try:
            url="http://ssex.com" + href
            name = href[1:]
            req=urllib.request.Request(url)
            resp=urllib.request.urlopen(req)
            html =resp.read().decode('utf-8')
            m3u8 = re.search('https:.*?playlist.m3u8', html).group()
            req=urllib.request.Request(m3u8)
            resp=urllib.request.urlopen(req)
            m3u8txt =resp.read().decode('utf-8')
            for i in ['720', '576','480', '360', '240']:
                tmp = re.search('x%s.*?m3u8' % i, m3u8txt.replace('\n', ''))
                if tmp is not None:
                    tail = tmp.group()[4:]
                    break
            req=urllib.request.Request(m3u8[:-13] + tail)
            resp=urllib.request.urlopen(req)
            ts_txt =resp.read().decode('utf-8')
            meadia = re.search('media_.*?.ts', ts_txt).group()
            ts_url = m3u8[:-13] + meadia
            new_cnt = int(meadia.split('_')[-1].split('.')[0])

            for i in range(new_cnt, new_cnt + forward):
                date = time.strftime("%Y-%m-%d", time.localtime())
                ts_url = m3u8[:-13] + meadia.replace(str(new_cnt), str(i))
                output_path = os.path.join(output_dir, name, date, str(i) + '.ts')
                if not os.path.exists(os.path.join(output_dir, name, date)) and bucket is None:
                    os.makedirs(os.path.join(output_dir, name, date))
                if (not os.path.exists(output_path)) or (os.path.getsize(output_path) == 0):
                    cmd = 'curl %s --output %s' % (ts_url, output_path)
                    qiniu_path = os.path.join(name, date, str(i) + '.ts')
                    download_ts(cmd, bucket, qiniu_path)
                    time.sleep(0.1)

        except Exception as e:
            print(e)
            

if __name__ == '__main__':
    while(1):
        hrefs = set()
        first_hrefs = None
        for i in range(1, 5):
            url = args.url
            url += ('?page=' + str(i))
            resp=requests.get(url)
            if resp.status_code==requests.codes.ok:
                    html=etree.HTML(resp.text)
                    tmp_hrefs=html.xpath('//ul[@class="list"]/li/a/@href')
                    if first_hrefs is None:
                        first_hrefs = tmp_hrefs
                    elif first_hrefs == tmp_hrefs:
                        break
                    hrefs.update(tmp_hrefs)
            else:
                continue
        hrefs = list(hrefs)
        cpus = args.cpus
        pool = Pool(processes=cpus)
        lin = np.linspace(0, len(hrefs), cpus + 1, dtype=np.int)
        for i in range(cpus):
            pool.apply_async(main, args=(hrefs[lin[i]: lin[i + 1]], args.bucket,))

        pool.close()
        pool.join()