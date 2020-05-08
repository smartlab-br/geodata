import shapefile
import json
import os
import sys
import pandas as pd
from geojson_rewind import rewind

datasets = [
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
    }
]

# Saving partition
def make_partition(buffer, f_name, group, in_group_id, identifier, cluster_identifier):
    os.makedirs(os.path.dirname(f_name), exist_ok=True)
    feats = []
    for feature in buffer:
        if feature.get('properties').get(identifier) in list(group[cluster_identifier].astype(str).unique()):
            if isinstance(feature.get('geometry').get('coordinates'), tuple):
                feature['geometry']['coordinates'] = list(feature.get('geometry').get('coordinates'))
                feats.append(list(rewind(feature)))
            else: 
                feats.append(rewind(feature))
    with open(f_name, "w") as geojson:
        json.dump({"type": "FeatureCollection", "features": feats}, geojson)
        total_done = total_done + 1
        print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
        # geojson.close() # Just to make sure it releases memory

# Converting files
def convert_shp(clusters, skip_existing, dataset, extension=None):
    global total_files, total_done
    total_files = total_files + 1
    print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
    # read the shapefile
    reader = shapefile.Reader(f"../../shapes/{dataset.get('origin')}/{dataset.get('file')}.shp")
    #reader.encoding="iso-8859-1"
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    buffer = []
    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        geom = sr.shape.__geo_interface__
        buffer.append(dict(type="Feature", geometry=geom, properties=atr)) 

    # Change au_type by extension
    au_type = dataset.get('au_type')
    if extension is not None:
        au_type = au_type.replace("regic", f"regic_{extension}")

    # write the GeoJSON files
    # Brazil
    if not (skip_existing and os.path.isfile(f'../../geojson/br/{au_type}_q0.json')):
        with open(f'../../geojson/br/{au_type}_q0.json', "w") as geojson:
            json.dump({"type": "FeatureCollection", "features": buffer}, geojson)
            total_done = total_done + 1
            print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
            # geojson.close() # Just to make sure it releases memory

    levels = {
        'region': 'macrorregiao',
        'uf': 'uf',
        'meso': 'mesorregiao',
        'micro': 'microrregiao'
    }
    
    for col, level in levels.items():
        grouped = clusters.groupby(col)
        total_files = total_files + len(grouped)
        print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
        for id_part, part in grouped:
            f_name = f'../../geojson/br/{level}/{au_type}/{id_part}_q0.json'
            if skip_existing and os.path.isfile(f_name):   
                continue            
            make_partition(buffer, f_name, part, col, dataset.get('identifier'), dataset.get('cluster_identifier'))

    return

def generate_regic(skip_existing):
    global total_files, total_done

    # Evaluate REGIC data as provided by IBGE
    clusters = pd.read_excel('REGIC2018_Regionalizacao_Saude_Primeira_Aproximacao.xlsx')
    clusters.columns = ['cd_mun', 'nm_mun', 'pop18', 'cd_baixa_media', 'nm_baixa_media', 'cd_alta', 'nm_alta']

    clusters = clusters.assign(
        uf = clusters['cd_mun'].astype(str).str.slice(0,2),
        region = clusters['cd_mun'].astype(str).str.slice(0,1)
    )

    with open("analysis_units_municipio.json", "r") as f_au:
        list_municipio = json.load(f_au)
        # f_au.close() # Just to make sure it releases memory
    df_municipio = pd.DataFrame.from_dict({
        'cd_mun': [mun.get('id') for mun in list_municipio],
        'micro': [mun.get('microrregiao', {}).get('id') for mun in list_municipio],
        'meso': [mun.get('microrregiao', {}).get('mesorregiao', {}).get('id') for mun in list_municipio]
    })
    clusters = clusters.join(df_municipio.set_index('cd_mun'), on="cd_mun")

    # Evaluate clusters with all municipalities, including those absent from IBGE's REGIC table
    if os.path.isfile('REGIC_melt.csv'):
        clusters_ext = pd.read_csv('REGIC_melt.csv')
        clusters_ext = clusters_ext.drop(['exerce_influencia_regic','influenciado_regic','presta_alta','presta_baixa','procura_servicos_alta','procura_servicos_baixa','grau_infl_recebe','grau_infl_exerce'], axis=1)
        clusters_ext = clusters_ext.pivot_table(index=['cd_mun_origem'], columns='tp_rel', values='cd_mun_dest', fill_value=0).reset_index()
        clusters_ext.columns = ['cd_mun', 'cd_alta', 'cd_baixa_media', 'cd_influencia']
        clusters_ext = clusters_ext.assign(
            uf = clusters['cd_mun'].astype(str).str.slice(0,2),
            region = clusters['cd_mun'].astype(str).str.slice(0,1)
        )
        clusters_ext = clusters_ext.drop_duplicates()
        clusters_ext = clusters_ext.join(df_municipio.set_index('cd_mun'), on="cd_mun")

    for dataset in datasets:
        convert_shp(clusters, True, dataset)
        if os.path.isfile('REGIC_melt.csv'):
            convert_shp(clusters_ext, skip_existing, dataset, "ext")

def generate_census(skip_existing):
    global total_files, total_done
    # TODO 1 - Generate BR geojsons from distritos and setores_censitários shapes
    pass

print("Starting conversion to geojson...", end='\r', flush=True)
if sys.argv[1] is None:
    skip_existing = True
else:
    skip_existing = sys.argv[1].lower() in ['true', '1', 't', 'y', 'yes']

total_files = 0
total_done = 0

generate_regic(skip_existing)
generate_census(skip_existing)

print(f"All shapes converted to geojson!!!!")