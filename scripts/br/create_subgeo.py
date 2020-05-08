import pandas as pd
import json
import os

# Resolutions rules
# TODO 1 - Map identifiers in resolutions BR geojsons
resolutions = {
    'municipio': {
        'levels': [
            'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
            'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
            'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade', 'regic_saude_baixamediaalta_complexidade',
            'regic_ext_saude_alta_complexidade', 'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade'
        ],
        'identifier': ''
    },
    'distrito': {
        'levels': [
            'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
            'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
            'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade', 'regic_saude_baixamediaalta_complexidade',
            'regic_ext_saude_alta_complexidade', 'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
            'macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'
        ],
        'identifier': ''
    },
    'setor_censitario': {
        'levels': [
            'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
            'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
            'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade', 'regic_saude_baixamediaalta_complexidade',
            'regic_ext_saude_alta_complexidade', 'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
            'macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio', 'distrito'
        ],
        'identifier': '',
        'filters': [
            'aglomerados_subnormais',
        ]
    }
}
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
    'regic_ext_saude_baixamediaalta_complexidade': 'cd_media_baixa_ext'
} # Ommited keys are considered to have key == value in the semantics of this script

def load_places():
    # Granularity = setor_censitario
    # TODO 2 - Start from setor_censitario, then join with distritos (already loaded in the code below)
    # TODO 3 - Add filters as columns, such as aglomerados subnormais
    print("Starting conversion to geojson...", end='\r', flush=True)

    with open("analysis_units_distritos.json", "r") as f_au:
        list_distrito = json.load(f_au)
        # f_au.close() # Just to make sure it releases memory
    df_distrito = pd.DataFrame.from_dict({
        'distrito': [dist.get('id') for dist in list_distrito],
        'municipio': [dist.get('municipio', {}).get('id') for dist in list_distrito],
        'microrregiao': [dist.get('municipio', {}).get('microrregiao', {}).get('id') for dist in list_distrito],
        'mesorregiao': [dist.get('municipio', {}).get('microrregiao', {}).get('mesorregiao', {}).get('id') for dist in list_distrito]
    })
    df_distrito = df_distrito.assign(
        uf = df_distrito['municipio'].astype(str).str.slice(0,2),
        macrorregiao = df_distrito['municipio'].astype(str).str.slice(0,1)
    )

    # Evaluate REGIC data as provided by IBGE
    clusters = pd.read_excel('REGIC2018_Regionalizacao_Saude_Primeira_Aproximacao.xlsx')
    clusters.columns = ['municipio', 'nm_mun', 'pop18', 'cd_baixa_media', 'nm_baixa_media', 'cd_alta', 'nm_alta']

    df = df_distrito.join(clusters.set_index('municipio'), on="municipio")

    # Evaluate clusters with all municipalities, including those absent from IBGE's REGIC table
    if os.path.isfile('REGIC_melt.csv'):
        clusters_ext = pd.read_csv('REGIC_melt.csv')
        clusters_ext = clusters_ext.drop(['exerce_influencia_regic','influenciado_regic','presta_alta','presta_baixa','procura_servicos_alta','procura_servicos_baixa','grau_infl_recebe','grau_infl_exerce'], axis=1)
        clusters_ext = clusters_ext.pivot_table(index=['cd_mun_origem'], columns='tp_rel', values='cd_mun_dest', fill_value=0).reset_index()
        clusters_ext.columns = ['municipio', 'cd_alta_ext', 'cd_baixa_media_ext', 'cd_influencia_ext']
        clusters_ext = clusters_ext.drop_duplicates()
        df = df.join(clusters_ext.set_index('municipio'), on="municipio")
    # TODO 4 - Check why codes are being converted to float
    return df

# Saving partition
def make_partition(geo_br, f_name, group, identifier, cluster_identifier):
    os.makedirs(os.path.dirname(f_name), exist_ok=True)
    with open(f_name, "w") as geojson:
        json.dump(
            {"type": "FeatureCollection", 
             "features": [feature for feature in geo_br.get('features') if feature.get(identifier) in list(group[cluster_identifier].astype(str).unique())]
            },
            geojson
        )
        total_done = total_done + 1
        print(f"Converting to geojson: {total_done}/{total_files} [{int(total_done/total_files*100)}%]", end="\r", flush=True)
        # geojson.close() # Just to make sure it releases memory

# Load BR geojson for each resolution level
def generate(df, skip_existing):
    for res_id, res in resolutions.items():
        # read the geojson
        if os.path.isfile(f"../../geojson/br/{res_id}_q4.json"):
            f_name = f"../../geojson/br/{res_id}_q4.json"
        elif os.path.isfile(f"../../geojson/br/{res_id}_q0.json"):
            f_name = f"../../geojson/br/{res_id}_q0.json"
        else:
            continue
        with open(f_name, 'r') as json_file:
            geo = json.load(json_file)
        
        # Iterate over levels to filter the resolution geometries
        for level in res.levels:
            col = cluster_cols.get(level, level)
            grouped = df.groupby(col)
            for au_id, cluster in grouped:
                for id_part, part in grouped:
                    f_name = f'../../geojson/br/{level}/{res_id}/{id_part}_q0.json'
                    if skip_existing and os.path.isfile(f_name):   
                        continue
                    # Filter geometries and save
                    geo_br, f_name, group, identifier, cluster_identifier
                    make_partition(geo, f_name, part, res.get('identifier'), col)

if sys.argv[1] is None:
    skip_existing = True
else:
    skip_existing = sys.argv[1].lower() in ['true', '1', 't', 'y', 'yes']

df = load_places(skip_existing)
print(df)
# generate(df, skip_existing)