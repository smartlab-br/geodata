import topojson
import json
import os
import sys
import subprocess
from numpy import linspace
import datetime
import multiprocess
import logging
import logging.config
# from shapely.geos import TopologyException

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(level)
    
    return logger

topo_logger = setup_logger('topo_logger', 'log/log_error_topology.log', logging.WARNING)
logger = setup_logger('general_logger', 'log/log_error.log', logging.ERROR)

def convert(origin, dest, quality_levels, skip_existing):
    global total_files, total_done, topo_logger, logger
    # Skip if all levels have already been generated and the flag is set
    if skip_existing and all([os.path.isfile(dest.replace('q0', f'q{quality_key+1}')) for quality_key, quality_simpl in enumerate(quality_levels)]):
        total_done = total_done + len(quality_levels)
        print(f"Converting from geojson to topojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)       
        return
    
    with open(origin, 'r') as json_file:
        data = json.load(json_file)
        # json_file.close() # Just to make sure it releases memory
    try:
        tj_base = topojson.Topology(data.get('features'), presimplify = False, prequantize=False, topology=True)
    except TopologyException as te:
        topo_logger.error(f">>>>> Error in topology from file {origin} <<<<<", exc_info=True)
        return
    except Exception as e:
        topo_logger.error(f">>>>> Error in file {origin} <<<<<", exc_info=True)
        return
    
    if '_q0' not in origin: # Do not loop if quality is already set in geojson.
        if skip_existing and os.path.isfile(dest_q):
            total_done = total_done + 1
            print(f"Converting from geojson to topojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
            return
        tj = tj_base.toposimplify(quality_levels[-1])
        if not os.path.exists(os.path.dirname(dest)):
            os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "w") as topo_file:
            json.dump(tj.to_dict(), topo_file)
        total_done = total_done + 1
        print(f"Converting from geojson to topojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)           
        return
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
            # topo_file.close() # Just to make sure it releases memory
        total_done = total_done + 1
        print(f"Converting from geojson to topojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)       
    
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
total_files = 0
for root, dirs, files in os.walk(strt):
    path = root.replace("../../geojson", "")
    for file in files:
        if file.endswith(".json"):
            args.append(('../../geojson{}/{}'.format(path,file), '../../topojson{}/{}'.format(path,file), quality_levels, True))
            if '_q0' in file:
                total_files = total_files + len(quality_levels)
            else:
                total_files = total_files + 1

total_done = 0
print(f"Creating threads for topojson generation: {total_files}", end="\r", flush=True)       
with multiprocess.Pool(processes=8) as pool:
    pool.starmap(convert, args)
    # pool.close() # Just to make sure it releases memory

print(f"All topojson files generated!!!!")