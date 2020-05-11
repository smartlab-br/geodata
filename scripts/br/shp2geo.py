import shapefile
import json
import multiprocess
import os
import sys
import pandas as pd
from geojson_rewind import rewind

# Datasets to be converted to geojson
datasets = [
    # REGIC
    {
        "origin": "REGIC/bases_graficas/Indice_de_atracao_alta_complexidade_saude", 
        "file": "Indice_de_atracao_alta_complexidade_saude",
        "au_type": "regic_saude_alta_complexidade",
        "identifier": "Geocodigo",
        "cluster_identifier": "cd_alta"
    },
    {
        "origin": "REGIC/bases_graficas/Indice_de_atracao_baixa_media_complexidade_saude", 
        "file": "Indice_de_atracao_baixa_media_complexidade_saude",
        "au_type": "regic_saude_baixamedia_complexidade",
        "identifier": "Geocodigo",
        "cluster_identifier": "cd_baixa_media"
    },
    {
        "origin": "REGIC/bases_graficas/rede_de_baixaMedia_e_alta_complexidade", 
        "file": "rede_de_baixaMedia_e_alta_complexidade",
        "au_type": "regic_saude_baixamediaalta_complexidade",
        "identifier": "DESTINO",
        "cluster_identifier": "cd_baixa_media"
    },
    {
        "origin": "REGIC/bases_graficas/REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao", 
        "file": "REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao",
        "au_type": "regic_saude_regioes_alta_complexidade",
        "identifier": "CodReg",
        "cluster_identifier": "cd_alta"
    },
    {
        "origin": "REGIC/bases_graficas/REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao", 
        "file": "REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao",
        "au_type": "regic_saude_regioes_baixamedia_complexidade",
        "identifier": "cod_Reg",
        "cluster_identifier": "cd_baixa_media"
    },
    # REGIC, as extended by Smartlab
    {
        "origin": "REGIC/bases_graficas/Indice_de_atracao_alta_complexidade_saude", 
        "file": "Indice_de_atracao_alta_complexidade_saude",
        "au_type": "regic_ext_saude_alta_complexidade",
        "identifier": "Geocodigo",
        "cluster_identifier": "cd_alta_ext"
    },
    {
        "origin": "REGIC/bases_graficas/Indice_de_atracao_baixa_media_complexidade_saude", 
        "file": "Indice_de_atracao_baixa_media_complexidade_saude",
        "au_type": "regic_ext_saude_baixamedia_complexidade",
        "identifier": "Geocodigo",
        "cluster_identifier": "cd_baixa_media_ext"
    },
    {
        "origin": "REGIC/bases_graficas/rede_de_baixaMedia_e_alta_complexidade", 
        "file": "rede_de_baixaMedia_e_alta_complexidade",
        "au_type": "regic_ext_saude_baixamediaalta_complexidade",
        "identifier": "DESTINO",
        "cluster_identifier": "cd_baixa_media_ext"
    },
    {
        "origin": "REGIC/bases_graficas/REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao", 
        "file": "REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao",
        "au_type": "regic_ext_saude_regioes_alta_complexidade",
        "identifier": "CodReg",
        "cluster_identifier": "cd_alta_ext"
    },
    {
        "origin": "REGIC/bases_graficas/REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao", 
        "file": "REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao",
        "au_type": "regic_saude_regioes_baixamedia_complexidade",
        "identifier": "cod_Reg",
        "cluster_identifier": "cd_baixa_media_ext"
    },
    # Territorial organizations
    {
        "origin": "territorio/uf/distritos", 
        "au_type": "distrito",
        "identifier": "CD_GEOCODD",
        "cluster_identifier": "distrito"
    },
    {
        "origin": "territorio/uf/subdistritos", 
        "au_type": "subdistrito",
        "identifier": "CD_GEOCODS",
        "cluster_identifier": "subdistrito"
    },
    {
        "origin": "territorio/uf/setores_censitarios", 
        "au_type": "setor_censitario",
        "identifier": "CD_GEOCODDI"#,
        # "cluster_identifier": "setor_censitario"
    }
]

# Each resolution must be clustered into certain (greater) levels of topology
resolutions = {
    'regic_saude_regioes_alta_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'CodReg'
    },
    'regic_saude_regioes_baixamedia_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'cod_Reg'
    },
    'regic_saude_alta_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'Geocodigo'
    },
    'regic_saude_baixamedia_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'Geocodigo'
    },
    'regic_saude_baixamediaalta_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'DESTINO'
    },
    'regic_ext_saude_regioes_alta_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'CodReg'
    },
    'regic_ext_saude_regioes_baixamedia_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'cod_Reg'
    },
    'regic_ext_saude_alta_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'Geocodigo'
    },
    'regic_ext_saude_baixamedia_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'Geocodigo'
    },
    'regic_ext_saude_baixamediaalta_complexidade': {
        'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'],
        'identifier': 'DESTINO'
    },
    'municipio': {
        'levels': [
            'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
            'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
            'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade', 'regic_saude_baixamediaalta_complexidade',
            'regic_ext_saude_alta_complexidade', 'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade'
        ],
        'identifier': 'codarea'
    },
    'distrito': {
        'levels': [
            'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
            'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
            'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade', 'regic_saude_baixamediaalta_complexidade',
            'regic_ext_saude_alta_complexidade', 'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
            'macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'
        ],
        'identifier': 'CD_GEOCODD'
    },
    'subdistrito': {
        'levels': [
            'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
            'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
            'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade', 'regic_saude_baixamediaalta_complexidade',
            'regic_ext_saude_alta_complexidade', 'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
            'macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio','distrito'
        ],
        'identifier': 'CD_GEOCODS'
    }#,
    # 'setor_censitario': {
    #     'levels': [
    #         'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
    #         'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
    #         'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade', 'regic_saude_baixamediaalta_complexidade',
    #         'regic_ext_saude_alta_complexidade', 'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
    #         'macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio', 'distrito','subdistrito'
    #     ],
    #     'identifier': 'CD_GEOCODDI',
    #     'filters': ['aglomerados_subnormais']
    # }
}

# Correlation columns indicating the belongin of analysis units
cluster_cols: {
    'regic_saude_regioes_alta_complexidade': 'cd_alta',
    'regic_saude_regioes_baixamedia_complexidade': 'cd_media_baixa',
    'regic_saude_alta_complexidade': 'cd_alta',
    'regic_saude_baixamedia_complexidade': 'cd_media_baixa',
    'regic_saude_baixamediaalta_complexidade': 'cd_media_baixa',
    'regic_ext_saude_regioes_alta_complexidade': 'cd_alta_ext',
    'regic_ext_saude_regioes_baixamedia_complexidade': 'cd_media_baixa_ext',
    'regic_ext_saude_alta_complexidade': 'cd_alta_ext',
    'regic_ext_saude_baixamedia_complexidade': 'cd_media_baixa_ext',
    'regic_ext_saude_baixamediaalta_complexidade': 'cd_media_baixa_ext',
    'setor_censitario': 'subdistrito'
} # Ommited keys are considered to have key == value in the semantics of this script

# Load the levels correlations as a pandas dataframe
def load_places():
    # Granularity = setor_censitario
    print("Starting conversion to geojson...", end='\r', flush=True)

    with open("analysis_units_subdistritos.json", "r") as f_au:
        list_subdistrito = json.load(f_au)
        # f_au.close() # Just to make sure it releases memory
    df_subdistrito = pd.DataFrame.from_dict({
        'subdistrito': [sd.get('id') for sd in list_subdistrito],
        'distrito': [sd.get('distrito',{}).get('id') for sd in list_subdistrito],
        'municipio': [sd.get('distrito',{}).get('municipio', {}).get('id') for sd in list_subdistrito],
        'microrregiao': [sd.get('distrito',{}).get('municipio', {}).get('microrregiao', {}).get('id') for sd in list_subdistrito],
        'mesorregiao': [sd.get('distrito',{}).get('municipio', {}).get('microrregiao', {}).get('mesorregiao', {}).get('id') for sd in list_subdistrito]
    })
    df_subdistrito = df_subdistrito.assign(
        uf = df_subdistrito['municipio'].astype(str).str.slice(0,2),
        macrorregiao = df_subdistrito['municipio'].astype(str).str.slice(0,1)
    )

    # Evaluate REGIC data as provided by IBGE
    clusters = pd.read_excel('REGIC2018_Regionalizacao_Saude_Primeira_Aproximacao.xlsx')
    clusters.columns = ['municipio', 'nm_mun', 'pop18', 'cd_baixa_media', 'nm_baixa_media', 'cd_alta', 'nm_alta']

    df = df_subdistrito.join(clusters.set_index('municipio'), on="municipio")

    # Evaluate clusters with all municipalities, including those absent from IBGE's REGIC table
    if os.path.isfile('REGIC_melt.csv'):
        clusters_ext = pd.read_csv('REGIC_melt.csv')
        clusters_ext = clusters_ext.drop(['exerce_influencia_regic','influenciado_regic','presta_alta','presta_baixa','procura_servicos_alta','procura_servicos_baixa','grau_infl_recebe','grau_infl_exerce'], axis=1)
        clusters_ext = clusters_ext.pivot_table(index=['cd_mun_origem'], columns='tp_rel', values='cd_mun_dest', fill_value=0).reset_index()
        clusters_ext.columns = ['municipio', 'cd_alta_ext', 'cd_baixa_media_ext', 'cd_influencia_ext']
        clusters_ext = clusters_ext.drop_duplicates()
        df = df.join(clusters_ext.set_index('municipio'), on="municipio")
    # TODO 1 - Check why codes are being converted to float
    return df

# Saving partition
def make_partition(geo_br, f_name, group, identifier, cluster_identifier):
    global total_files, total_done
    os.makedirs(os.path.dirname(f_name), exist_ok=True)
    # TO_DELETE - Previous code, with rewind an conversion from tuple to list
    # for feature in buffer:
    #     if feature.get('properties').get(identifier) in list(group[cluster_identifier].astype(str).unique()):
    #         if isinstance(feature.get('geometry').get('coordinates'), tuple):
    #             feature['geometry']['coordinates'] = list(feature.get('geometry').get('coordinates'))
    #         feats.append(rewind(feature))
    # with open(f_name, "w") as geojson:
    #     json.dump({"type": "FeatureCollection", "features": feats}, geojson)
    with open(f_name, "w") as geojson:
        if identifier == 'CD_GEOCODDI': # Case setor_censitario, there's no listing beforehand - it assumes from subdistrito
            feats = [feature for feature in geo_br.get('features') if feature.get(identifier)[:11] in list(group[cluster_identifier].astype(str).unique())]
        else:
            feats = [feature for feature in geo_br.get('features') if feature.get(identifier) in list(group[cluster_identifier].astype(str).unique())]
        json.dump({"type": "FeatureCollection", "features": feats}, geojson)
    total_done = total_done + 1
    print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)

def read_geometries_from_shapefile(origin):
    reader = shapefile.Reader(origin)
    #reader.encoding="iso-8859-1"
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    buffer = []
    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        geom = sr.shape.__geo_interface__
        buffer.append(dict(type="Feature", geometry=geom, properties=atr)) 
    return buffer
    
def convert_as_is(dataset, skip_existing):
    global total_files, total_done
    
    total_files = total_files + 1
    print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
    # read the shapefile

    # write the GeoJSON files
    if 'regic' in dataset.get('au_type'): # Already in BR level
        # Brazil
        if not (skip_existing and os.path.isfile(f'../../geojson/br/{au_type}_q0.json')):
            with open(f'../../geojson/br/{au_type}_q0.json', "w") as geojson:
                buffer = read_geometries_from_shapefile(f"../../shapes/{dataset.get('origin')}/{dataset.get('file')}.shp")
                json.dump({"type": "FeatureCollection", "features": buffer}, geojson)
        total_done = total_done + 1
        print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
    else: # All the rest is in UF level - generate as it is and then join the features to a single, BR, geojson
        buffer = []
        # UF (iterate)
        for root, dirs, files in os.walk(f"../../geojson/br/uf/{au_type}"):
            # path = root.replace("../../geojson", "")
            for file in files:
                if file.endswith(".shp"):
                    au_id = file.replace('.shp', '')            
                    if not (skip_existing and os.path.isfile(f'../../geojson/br/uf/{au_type}/{au_id}_q0.json')):
                        with open(f'../../geojson/br/uf/{au_type}/{au_id}_q0.json', "w") as geojson:
                            local_buffer = read_geometries_from_shapefile(f"../../shapes/{dataset.get('origin')}/{dataset.get('file')}.shp")
                            json.dump({"type": "FeatureCollection", "features": buffer}, geojson)
                            buffer.extend(local_buffer)
                            # Teste
                            if au_id == '11':
                                print(au_id)
                                print(local_buffer)
                    total_done = total_done + 1
                    print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
        # Brazil
        if not (skip_existing and os.path.isfile(f'../../geojson/br/{au_type}_q0.json')):
            with open(f'../../geojson/br/{au_type}_q0.json', "w") as geojson:
                json.dump({"type": "FeatureCollection", "features": buffer}, geojson)
        total_done = total_done + 1
        print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
    return

# Generates new topologies by combining levels and resolutions
def generate(res_id, level, places, identifier, skip_existing):
    global total_files, total_done, cluster_cols

    # read the BR geojson
    if os.path.isfile(f"../../geojson/br/{res_id}_q4.json"):
        f_name = f"../../geojson/br/{res_id}_q4.json"
    elif os.path.isfile(f"../../geojson/br/{res_id}_q0.json"):
        f_name = f"../../geojson/br/{res_id}_q0.json"
    else:
        continue
    with open(f_name, 'r') as json_file:
        geo = json.load(json_file)
    
    col = cluster_cols.get(level, level)
    grouped = palces.groupby(col)
    for au_id, cluster in grouped:
        for id_part, part in grouped:
            f_name = f'../../geojson/br/{level}/{res_id}/{id_part}_q0.json'
            if skip_existing and os.path.isfile(f_name):   
                continue
            # Filter geometries and save
            make_partition(geo, f_name, part, identifier, col)
    return

print("Starting conversion to geojson...", end='\r', flush=True)
if sys.argv[1] is None:
    skip_existing = True
else:
    skip_existing = sys.argv[1].lower() in ['true', '1', 't', 'y', 'yes']

total_files = 0
total_done = 0

places = load_places()

pool_as_is = [] # Pool to address shp conversion to basic geojson
for dataset in datasets:
    pool_as_is.append((dataset, skip_existing))
with multiprocess.Pool(processes=8) as pool:
    pool.starmap(convert_as_is, pool_args)

# Generate combinations levels x geometries
pool_combinations = [] # Pool to address combination of levels and resolutions of geographies
for res_id, res in resolutions.items():
    # Iterate over levels to filter the resolution geometries
    for level in res.levels:
        pool_combinations.append((res_id, level, places, res.get('identifier'), skip_existing))
with multiprocess.Pool(processes=8) as pool:
    pool_combinations.starmap(generate, pool_args)

# TODO 2 - Create mechanism to filter a combination of level x resolution (check aglomerados subnormais)
# pool_args = generate_regic(skip_existing)
# with multiprocess.Pool(processes=8) as pool:
#     pool.starmap(convert_shp_regic, pool_args)

print(f"All shapes converted to geojson!!!!")