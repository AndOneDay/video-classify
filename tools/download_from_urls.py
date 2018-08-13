import os
from tqdm import tqdm
from multiprocessing import Pool
import numpy as np
import time
import pickle
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="multi thread download", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--prefix', type=str, default='', help="url prefix")
    parser.add_argument('--save_path',default='./',type=str,help="save path")
    parser.add_argument('--input_file',required=True,type=str,help="urls file pkl or lst")  
    parser.add_argument('--cpus',default=16,type=int,help="num of process")                      
    args = parser.parse_args()
    return args
    

args = parse_args()

def main(urls):
    prefix = args.prefix
    for url in tqdm(urls):
        cmd = 'wget ' + prefix + url + ' -P ' + args.save_path
        os.system(cmd)


if __name__=='__main__':
    cpus = args.cpus
    if args.input_file.endswith('pkl'):
        urls = pickle.load(open('./urls1.pkl', 'rb'))
    else:
        urls = []
        with open(args.input_file, 'r') as f:
            for line in f:
                if line.endswith('\n'):
                    line = line[:-1]
                urls.append(line)
    length = len(urls)
    #print(length)
    pool = Pool(processes=cpus)
    lin = np.linspace(0, length, cpus + 1, dtype=np.int)
    for i in range(cpus):
        pool.apply_async(main, args=(urls[lin[i]:lin[i+1]],))

    pool.close()
    pool.join()