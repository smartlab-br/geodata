import topojson
import json
import os
import subprocess
from numpy import linspace

def convert(origin, dest, quality_levels):
    with open(origin, 'r') as json_file:
        data = json.load(json_file)
        json_file.close()

    tj_base = topojson.Topology(data.get('features'), presimplify = False, prequantize=False, topology=True)
    for quality_key, quality_simpl in enumerate(quality_levels):
        print(f"Generating from {origin} with simplification {quality_simpl} ({quality_key + 1})")
        tj = tj_base.toposimplify(quality_simpl)
        dest_q = dest.replace('q0', f'q{quality_key+1}')
        if not os.path.exists(os.path.dirname(dest_q)):
            os.makedirs(os.path.dirname(dest_q), exist_ok=True)
        with open(dest_q, "w") as topo_file:
            topo_file.write(json.dumps(tj.to_json()))
            topo_file.close()

skip_existing = True
strt = "geojson"

quality_levels = list(linspace(0,0.01,4))
quality_levels.reverse()
print(quality_levels)

# for root, dirs, files in os.walk(strt):
#     path = root.replace("geojson", "")
#     for file in files:
#         origin = './geojson{}/{}'.format(path,file)
#         dest = './topojson{}/{}'.format(path,file)
#         if file.endswith(".json") and not (skip_existing and os.path.isfile(dest)):
#             try:
#                 convert(origin, dest, quality_levels)
#             except:
#                 continue
#             # subprocess.call(['geo2json',origin,'>',dest])
#             break

## Teste UF
# convert(
#     'geojson/br/uf/regic_saude_regioes_alta_complexidade/29_q0.json',
#     'topojson/br/uf/regic_saude_regioes_alta_complexidade/29_q0.json',
#     quality_levels
# )

## Teste BR
convert(
    'geojson/br/regic_saude_regioes_alta_complexidade_q0.json',
    'topojson/br/regic_saude_regioes_alta_complexidade_q0.json',
    quality_levels
)