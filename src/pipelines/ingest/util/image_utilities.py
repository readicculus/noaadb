import json
import os
from datetime import datetime


def safe_float_cast(s):
    if s is None: return None
    try:
        return float(s)
    except:
        return None


def safe_int_cast(s):
    if s is None: return None
    try:
        return int(s)
    except:
        return None


def file_key(fn):
    fn = os.path.basename(fn)
    return '_'.join(fn.split('_')[:-1])

KOTZ_MAPPINGS = {'CENT': 'C', 'LEFT': 'L', 'RIGHT': 'R', 'RGHT': 'R'}
def flight_cam_id_from_dir(path):
    # parse cam and flight id
    parts = str(path).split('/')
    while '' in parts: parts.remove('')
    flight_id, cam_id = parts[-2], KOTZ_MAPPINGS[parts[-1]]
    return flight_id, cam_id

def parse_chess_fn(file_name):
    name_parts = file_name.split('_')
    if name_parts[0] == 'CHESS':
        start_idx = 1

        flight = name_parts[start_idx]
        cam = name_parts[start_idx + 1]
        t = name_parts[start_idx + 2]
        t2 = name_parts[start_idx + 3]
        ms = t2.split('.')[1]
        date = t2.split('.')[0]
        ts = datetime.strptime(t + date, "%y%m%d%H%M%S").timestamp() + float('.' + ms)
        timestamp = datetime.fromtimestamp(ts)
    # elif name_parts[1] == 'polar':
    #     # files like '010_polar_bear_2019_fl10_R_20190516_214404.532608'
    #     return None,None,None

    else:
        start_idx = 3
        # fl01 images have slightly  different names
        if name_parts[2] != '2019':
            start_idx = 2
        flight = name_parts[start_idx]
        cam = name_parts[start_idx + 1]
        t = name_parts[start_idx + 3]
        ms = t.split('.')[1].replace('GMT', '')
        date = t.split('.')[0]
        ts = datetime.strptime(date, "%Y%m%d%H%M%S").timestamp() + float('.' + ms)
        timestamp = datetime.fromtimestamp(ts)
    return flight, cam, timestamp


def parse_kotz_filename(fn):
    name_parts = fn.split('_')
    start_idx = 3
    # fl01 images have slightly  different names
    if name_parts[2] != '2019':
        start_idx = 2
    flight = name_parts[start_idx]
    cam, day, time = name_parts[start_idx + 1], name_parts[start_idx + 2], name_parts[start_idx + 3]
    time, ms = time.split('.')
    day_hr_ms = day + '_' + time
    ts = datetime.strptime(day_hr_ms, "%Y%m%d_%H%M%S").timestamp() + float('.' + ms)
    timestamp = datetime.fromtimestamp(ts)
    return flight, cam, timestamp

def parse_beaufort_filename(fn):
    name_parts = fn.split('_')
    start_idx = 3
    # fl01 images have slightly  different names
    if name_parts[2] != '2019':
        start_idx = 2
    flight = name_parts[start_idx]
    cam, day, time = name_parts[start_idx + 1], name_parts[start_idx + 2], name_parts[start_idx + 3]
    time, ms = time.split('.')
    day_hr_ms = day + '_' + time
    ts = datetime.strptime(day_hr_ms, "%Y%m%d_%H%M%S").timestamp() + float('.' + ms)
    timestamp = datetime.fromtimestamp(ts)
    return flight, cam, timestamp

# MetaFile Parser
class MetaParser:
    def __init__(self, fn):
        with open(fn, 'r') as f:
            meta = f.read()
            self.json = json.loads(meta)

        self.ir = self.json.get('ir') or {}
        self.rgb = self.json.get('rgb') or {}
        self.evt = self.json.get('evt') or {}
        self.ins = self.json.get('ins') or {}

    def get_header(self):
        if 'header' in self.rgb:
            return self.rgb['header']
        if 'header' in self.ir:
            return self.ir['header']
        if 'header' in self.ins:
            return self.ins['header']
        if 'header' in self.evt:
            return self.evt['header']

    def get_ins(self):
        return self.ins

    def get_image(self, im_type):
        if im_type == 'rgb':
            return self.rgb
        if im_type == 'ir':
            return self.ir
        return None
