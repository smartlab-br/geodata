import topojson
import json
import os
import subprocess
from numpy import linspace
import datetime
# import multiprocess

def convert(origin, dest, quality_levels, skip_existing):
    # Skip if all levels have already been generated and the flag is set
    if skip_existing and all([os.path.isfile(dest.replace('q0', f'q{quality_key+1}')) for quality_key, quality_simpl in enumerate(quality_levels)]):
        return
    
    print(f"Reading from {origin}")
    with open(origin, 'r') as json_file:
        data = json.load(json_file)
        json_file.close()
    print(f"Generating topology @ {datetime.datetime.now()}")
    tj_base = topojson.Topology(data.get('features'), presimplify = False, prequantize=False, topology=True)
    print(f"Topology created successfully @ {datetime.datetime.now()}")
    for quality_key, quality_simpl in enumerate(quality_levels):
        dest_q = dest.replace('q0', f'q{quality_key+1}')
        # Skips if destination fila already exists and the flag is set
        if skip_existing and os.path.isfile(dest_q):
            continue
        print(f"Generating topojson with simplification {quality_simpl} ({quality_key + 1})")
        tj = tj_base.toposimplify(quality_simpl)
        if not os.path.exists(os.path.dirname(dest_q)):
            os.makedirs(os.path.dirname(dest_q), exist_ok=True)
        with open(dest_q, "w") as topo_file:
            json.dump(tj.to_dict(), topo_file)
            topo_file.close()
    print(f"Files generated from {origin} @ {datetime.datetime.now()}")

skip_existing = True
strt = "../../geojson"

print(f"Starting... @ {datetime.datetime.now()}")

quality_levels = list(linspace(0,0.01,4))
quality_levels.reverse()
print(f"Simplification levels: {quality_levels}")

# for root, dirs, files in os.walk(strt):
#     path = root.replace("geojson", "")
#     for file in files:
#         origin = '../../geojson{}/{}'.format(path,file)
#         dest = '../../topojson{}/{}'.format(path,file)
#         if file.endswith(".json"):
#             try:
#                 convert(origin, dest, quality_levels, True)
#             except:
#                 continue

## Multithread alternative
with multiprocessing.Pool(processes=4) as pool:
    for root, dirs, files in os.walk(strt):
        path = root.replace("geojson", "")
        pool.starmap(convert, ('../../geojson{}/{}'.format(path,file), '../../topojson{}/{}'.format(path,file), quality_levels, True))
    # pool.close()

## Teste UF
# convert(
#     '../../geojson/br/uf/regic_saude_regioes_alta_complexidade/29_q0.json',
#     '../../topojson/br/uf/regic_saude_regioes_alta_complexidade/29_q0.json',
#     quality_levels,
#     False
# )

## Teste BR
# convert(
#     '../../geojson/br/regic_saude_regioes_alta_complexidade_q0.json',
#     '../../topojson/br/regic_saude_regioes_alta_complexidade_q0.json',
#     quality_levels,
#     False
# )

print(f"Done!!!! @ {datetime.datetime.now()}")