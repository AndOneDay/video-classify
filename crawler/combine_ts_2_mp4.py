import os
from tqdm import tqdm
from multiprocessing import Pool
import numpy as np
import time

def parse_args():
    parser = argparse.ArgumentParser(description="ts crawler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--ts_dir', type=str, required=True, help="original ts dir")
    parser.add_argument('--output_dir',type=str, required=True, help="output dir")
    parser.add_argument('--cpus',default=16,type=int,help="num of process")                     
    args = parser.parse_args()
    return args   

args = parse_args()

def get_all_subdir(base_path):
    all_subdir = []
    rooms = os.listdir(base_path)
    for room in rooms:
        room_path = os.path.join(base_path, room)
        if os.path.isdir(room_path):
            dates = os.listdir(room_path)
            for date in dates:
                all_subdir.append((os.path.join(room_path, date), room))
        else:
            print(room_path)
    return all_subdir


def main(sub_pathes, length, output_dirs=''):
    for sub_path, room in tqdm(sub_pathes):
        if not os.path.isdir(sub_path):
            continue
        files = os.listdir(sub_path)
        for f in files:
            if f.find('tmp') > -1:
                end = f.rfind('.')
                os.rename(os.path.join(sub_path, f), os.path.join(sub_path, f[:end]))
        files = os.listdir(sub_path)
        try:
            files.sort(key=lambda x: int(x[:-3]))
        except Exception as e:
            print(e)
            print(sub_path)

        fs = []
        cnt = 0
        for i in range(len(files)):
            if (len(fs) != 0 and int(files[i][:-3]) != int(files[i - 1][:-3]) + 1) or \
                    len(fs) >= 5 or i == len(files) - 1:
                combine_ts(sub_path, fs, cnt, output_dirs, room)
                cnt += 1
                fs.clear()
            fs.append(os.path.join(sub_path, files[i]))


def combine_ts(base_path, fs, index, output_dir, room):
    #ffmpeg -i "concat:input1.ts|input2.ts|input3.ts" -c copy -bsf:a aac_adtstoasc -movflags +faststart output.mp4
    cmd = 'ffmpeg -i \"concat:'
    for i in range(len(fs)):
        if i != 0:
            cmd += '|'
        cmd += fs[i]
    cmd += '\" -map v:0 -c:v libx264 -crf 18 -pix_fmt yuv420p -g 5 -profile:v high '
    date = time.strftime("%Y-%m-%d", time.localtime())
    cmd += os.path.join(output_dir, (room + '-' + date + '-' + str(index) + '.mp4'))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if len(fs) > 256:
        print(len(fs))
        os.system('ulimit -n 1024 && %s' % cmd)
    else:
        os.system(cmd)


if __name__=='__main__':
    cpus = args.cpus
    base_path = args.ts_dir
    sub_pathes = get_all_subdir(base_path)
    length = len(sub_pathes)
    #print(length)
    pool = Pool(processes=cpus)
    lin = np.linspace(0, length, cpus + 1, dtype=np.int)
    output_dir = args.output_dir
    basepath_len = len(base_path)
    for i in range(cpus):
        pool.apply_async(main, args=(sub_pathes[lin[i]:lin[i+1]], basepath_len, output_dir,))

    pool.close()
    pool.join()