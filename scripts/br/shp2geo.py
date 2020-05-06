import shapefile
import json
import os
import pandas as pd
from geojson_rewind import rewind

datasets = [
    {
        "origin": "REGIC/bases_graficas/Indice_de_atracao_alta_complexidade_saude", 
        "file": "Indice_de_atracao_alta_complexidade_saude",
        "au_type": "regic_saude_alta_complexidade",
        "identifier": "Geocodigo",
        "cluster_identifer": "cd_alta"
    },
    {
        "origin": "REGIC/bases_graficas/Indice_de_atracao_baixa_media_complexidade_saude", 
        "file": "Indice_de_atracao_baixa_media_complexidade_saude",
        "au_type": "regic_saude_baixamedia_complexidade",
        "identifier": "Geocodigo",
        "cluster_identifer": "cd_baixa_media"
    },
    {
        "origin": "REGIC/bases_graficas/rede_de_baixaMedia_e_alta_complexidade", 
        "file": "rede_de_baixaMedia_e_alta_complexidade",
        "au_type": "regic_saude_baixamediaalta_complexidade",
        "identifier": "DESTINO",
        "cluster_identifer": "cd_baixa_media"
    },
    {
        "origin": "REGIC/bases_graficas/REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao", 
        "file": "REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao",
        "au_type": "regic_saude_regioes_alta_complexidade",
        "identifier": "CodReg",
        "cluster_identifer": "cd_alta"
    },
    {
        "origin": "REGIC/bases_graficas/REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao", 
        "file": "REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao",
        "au_type": "regic_saude_regioes_baixamedia_complexidade",
        "identifier": "cod_Reg",
        "cluster_identifer": "cd_baixa_media"
    }
]

# Saving partition
def make_partition(buffer, group_name, group, in_group_id, skip_existing, level, res, identifier, cluster_identifer):
    f_name = f'../../geojson/br/{level}/{res}/{group_name}_q0.json'
    if skip_existing and os.path.isfile(f_name):
        return
    os.makedirs(os.path.dirname(f_name), exist_ok=True)
    geojson = open(f_name, "w")
    geojson.write(
        json.dumps(
            rewind({
                "type": "FeatureCollection", 
                "features": [feature for feature in buffer if feature.get('properties').get(identifier) in list(group[cluster_identifer].astype(str).unique())]
            })
        )
    )
    geojson.close()

# Converting files
def convert_shp(clusters, skip_existing, path, f_name, au_type, identifier, cluster_identifer):
    # read the shapefile
    print(f'Reading from {f_name}')
    reader = shapefile.Reader(f'../../shapes/{path}/{f_name}.shp')
    #reader.encoding="iso-8859-1"
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    buffer = []
    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        geom = sr.shape.__geo_interface__
        buffer.append(dict(type="Feature", geometry=geom, properties=atr)) 

    # write the GeoJSON files
    # Brazil
    print("Storing country")
    if not (skip_existing and os.path.isfile(f'../../geojson/br/{au_type}_q0.json')):
        geojson = open(f'../../geojson/br/{au_type}_q0.json', "w")
        geojson.write(json.dumps({"type": "FeatureCollection", "features": buffer}))
        geojson.close()

    levels = {
        'region': 'macrorregiao',
        'uf': 'uf',
        'meso': 'mesorregiao',
        'micro': 'microrregiao'
    }
    for col, level in levels.items():
        print(f"Splitting into {level}")
        grouped = clusters.groupby(col)
        for id_part, part in grouped:
            make_partition(buffer, id_part, part, col, skip_existing, level, au_type, identifier, cluster_identifer)

    return

print("Starting...")
clusters = pd.read_excel('REGIC2018_Regionalizacao_Saude_Primeira_Aproximacao.xlsx')
clusters.columns = ['cd_mun', 'nm_mun', 'pop18', 'cd_baixa_media', 'nm_baixa_media', 'cd_alta', 'nm_alta']

clusters = clusters.assign(
    uf = clusters['cd_mun'].astype(str).str.slice(0,2),
    region = clusters['cd_mun'].astype(str).str.slice(0,1)
)

f_au = open("analysis_units_municipio.json", "r")
list_municipio = json.loads(f_au.read())
f_au.close()
df_municipio = pd.DataFrame.from_dict({
    'cd_mun': [mun.get('id') for mun in list_municipio],
    'micro': [mun.get('microrregiao', {}).get('id') for mun in list_municipio],
    'meso': [mun.get('microrregiao', {}).get('mesorregiao', {}).get('id') for mun in list_municipio]
})
clusters = clusters.join(df_municipio.set_index('cd_mun'), on="cd_mun")

for dataset in datasets:
    convert_shp(clusters, True, dataset.get('origin'), dataset.get('file'), dataset.get('au_type'), dataset.get('identifier'), dataset.get('cluster_identifer'))
print("Done!")