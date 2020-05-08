import topojson
import json
import os
import sys
import subprocess
from numpy import linspace
import datetime
import multiprocess

def convert(origin, dest, quality_levels, skip_existing):
    global total_files, total_done
    # Skip if all levels have already been generated and the flag is set
    if skip_existing and all([os.path.isfile(dest.replace('q0', f'q{quality_key+1}')) for quality_key, quality_simpl in enumerate(quality_levels)]):
        total_done = total_done + len(quality_levels)
        print(f"Converting from geojson to topojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)       
        return
    
    with open(origin, 'r') as json_file:
        data = json.load(json_file)
        # json_file.close() # Just to make sure it releases memory
    tj_base = topojson.Topology(data.get('features'), presimplify = False, prequantize=False, topology=True)
    
    for quality_key, quality_simpl in enumerate(quality_levels):
        dest_q = dest.replace('q0', f'q{quality_key+1}')
        # Skips if destination fila already exists and the flag is set
        if skip_existing and os.path.isfile(dest_q):
            total_done = total_done + 1
            print(f"Converting from geojson to topojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)       
            continue
        tj = tj_base.toposimplify(quality_simpl)
        if not os.path.exists(os.path.dirname(dest_q)):
            os.makedirs(os.path.dirname(dest_q), exist_ok=True)
        with open(dest_q, "w") as topo_file:
            json.dump(tj.to_dict(), topo_file)
            total_done = total_done + 1
            print(f"Converting from geojson to topojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)       
            # topo_file.close() # Just to make sure it releases memory
    
if sys.argv[1] is None:
    skip_existing = True
else:
    skip_existing = sys.argv[1].lower() in ['true', '1', 't', 'y', 'yes']

strt = "../../geojson"

print(f"Starting conversion from geojson to topojson...", end='\r', flush=True)

quality_levels = list(linspace(0,0.01,4))
quality_levels.reverse()

## Multithread alternative
args=[]
for root, dirs, files in os.walk(strt):
    path = root.replace("../../geojson", "")
    for file in files:
        if file.endswith(".json"):
            args.append(('../../geojson{}/{}'.format(path,file), '../../topojson{}/{}'.format(path,file), quality_levels, True))

total_files = len(args) * len(quality_levels)
total_done = 0
print(f"Converting from geojson to topojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)       
with multiprocess.Pool(processes=8) as pool:
    pool.starmap(convert, args)
    # pool.close() # Just to make sure it releases memory

print(f"All topojson files generated!!!!")