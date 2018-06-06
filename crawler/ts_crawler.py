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
    args = parser.parse_args()
    return args
    


args = parse_args()

def download_ts(cmd, method='local', qiniu_path='', bucket='ts02'):
    if method == 'local':
        os.system(cmd)
    elif method == 'qiniu':
        if qiniu_path == '':
            return
        tmp = cmd.split(' ')
        url = tmp[1]
        os.system("qshell account elYxU2yfITGA45M2dL1up4k9IYc1ehgRWzrh22Sk PxfV1B2Mfy4NuT0OuDdnjGIe7V478kJ6v8wxwUy_")
        cmd = 'qshell fetch ' + url + ' ' + bucket + ' ' + qiniu_path
        os.system(cmd)
        

def main(hrefs, start, end, output_dir='/workspace/run/data/ts_qiniu_many', forward=5, down_list=100):

    for k in range(start, end):
        try:
            url="http://ssex.com" + hrefs[k]
            name = hrefs[k][1:]
            req=urllib.request.Request(url)
            resp=urllib.request.urlopen(req)
            html =resp.read().decode('utf-8')
            m3u8 = re.search('https:.*?playlist.m3u8', html).group()
            req=urllib.request.Request(m3u8)
            resp=urllib.request.urlopen(req)

            for i in ['720', '576','480', '360', '240']:
                tmp = re.search('x%s.*?m3u8' % i, m3u8txt.replace('\n', ''))
                if tmp is not None:
                    tail = tmp.group()[4:]
                    #print(tail)
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
                    if not os.path.exists(os.path.join(output_dir, name, date)):
                        os.makedirs(os.path.join(output_dir, name, date))
                    if (not os.path.exists(output_path)) or (os.path.getsize(output_path) == 0):
                        cmd = 'curl %s --output %s' % (ts_url, output_path)
                        qiniu_path = os.path.join(name, date, str(i) + '.ts')
                        download_ts(cmd, 'qiniu', qiniu_path)
                        #print('download,', i, end='\r')
                        time.sleep(0.1)

        except Exception as e:
            print(e)
            

if __name__ == '__main__':
    while(1):
        resp=requests.get(args.url)
        if resp.status_code==requests.codes.ok:
                html=etree.HTML(resp.text)
                hrefs=html.xpath('//ul[@class="list"]/li/a/@href')
    #             for href in hrefs:
    #                 print(href)      
        else:
            continue

        pool = Pool(processes=args.cpus)
        lin = np.linspace(0, len(hrefs), cpus + 1, dtype=np.int)
        for i in range(cpus):
            pool.apply_async(main, args=(hrefs, lin[i], lin[i + 1],))

        pool.close()
        pool.join()