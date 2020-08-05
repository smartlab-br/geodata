import shapefile
import json
import multiprocess
import os
import sys
import pandas as pd
import requests


class Shape2Geo:
    # Datasets to be converted to geojson
    DATASETS = [
        # REGIC
        {
            "origin": "REGIC/Indice_de_atracao_alta_complexidade_saude",
            "file": "Indice_de_atracao_alta_complexidade_saude",
            "au_type": "regic_saude_alta_complexidade",
            "identifier": "Geocodigo",
            "cluster_identifier": "cd_alta"
        },
        {
            "origin": "REGIC/Indice_de_atracao_baixa_media_complexidade_saude",
            "file": "Indice_de_atracao_baixa_media_complexidade_saude",
            "au_type": "regic_saude_baixamedia_complexidade",
            "identifier": "Geocodigo",
            "cluster_identifier": "cd_baixa_media"
        },
        {
            "origin": "REGIC/rede_de_baixaMedia_e_alta_complexidade",
            "file": "rede_de_baixaMedia_e_alta_complexidade",
            "au_type": "regic_saude_baixamediaalta_complexidade",
            "identifier": "DESTINO",
            "cluster_identifier": "cd_baixa_media"
        },
        {
            "origin": "REGIC/REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao",
            "file": "REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao",
            "au_type": "regic_saude_regioes_alta_complexidade",
            "identifier": "CodReg",
            "cluster_identifier": "cd_alta"
        },
        {
            "origin": "REGIC/REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao",
            "file": "REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao",
            "au_type": "regic_saude_regioes_baixamedia_complexidade",
            "identifier": "cod_Reg",
            "cluster_identifier": "cd_baixa_media"
        },
        # REGIC, as extended by Smartlab
        {
            "origin": "REGIC/Indice_de_atracao_alta_complexidade_saude",
            "file": "Indice_de_atracao_alta_complexidade_saude",
            "au_type": "regic_ext_saude_alta_complexidade",
            "identifier": "Geocodigo",
            "cluster_identifier": "cd_alta_ext"
        },
        {
            "origin": "REGIC/Indice_de_atracao_baixa_media_complexidade_saude",
            "file": "Indice_de_atracao_baixa_media_complexidade_saude",
            "au_type": "regic_ext_saude_baixamedia_complexidade",
            "identifier": "Geocodigo",
            "cluster_identifier": "cd_baixa_media_ext"
        },
        {
            "origin": "REGIC/rede_de_baixaMedia_e_alta_complexidade",
            "file": "rede_de_baixaMedia_e_alta_complexidade",
            "au_type": "regic_ext_saude_baixamediaalta_complexidade",
            "identifier": "DESTINO",
            "cluster_identifier": "cd_baixa_media_ext"
        },
        {
            "origin": "REGIC/REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao",
            "file": "REGIC2018_Regioes_Alta_complexidade_saude_1aproximacao",
            "au_type": "regic_ext_saude_regioes_alta_complexidade",
            "identifier": "CodReg",
            "cluster_identifier": "cd_alta_ext"
        },
        {
            "origin": "REGIC/REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao",
            "file": "REGIC2018_Regioes_BaixaMedia_complexidade_saude_1aproximacao",
            "au_type": "regic_saude_regioes_baixamedia_complexidade",
            "identifier": "cod_Reg",
            "cluster_identifier": "cd_baixa_media_ext"
        },
        # Territorial organizations
        # TODO - Add macro-regions
        {
            "origin": "territorio",
            "file": "unidades_da_federacao",
            "au_type": "uf",
            "identifier": "CD_GEOCUF",
            "cluster_identifier": "uf"
        },
        {
            "origin": "territorio",
            "file": "mesorregioes",
            "au_type": "mesorregiao",
            "identifier": "CD_GEOCME",
            "cluster_identifier": "mesorregiao"
        },
        {
            "origin": "territorio",
            "file": "microrregioes",
            "au_type": "microrregiao",
            "identifier": "CD_GEOCMI",
            "cluster_identifier": "microrregiao"
        },
        {
            "origin": "territorio",
            "file": "municipios",
            "au_type": "municipio",
            "identifier": "CD_GEOCMU",
            "cluster_identifier": "municipio"
        },
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
            "identifier": "CD_GEOCODI",
            "cluster_identifier": "setor_censitario"
        }
    ]

    # Each resolution must be clustered into certain (greater) levels of topology
    RESOLUTIONS = {
        'regic_saude_regioes_alta_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'CodReg',
            'namer': 'Nome_Reg'
        },
        'regic_saude_regioes_baixamedia_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'cod_Reg',
            'namer': 'Nome_REG'
        },
        'regic_saude_alta_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'Geocodigo',
            'namer': 'NOME_CIDAD'
        },
        'regic_saude_baixamedia_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'Geocodigo',
            'namer': 'NOME_CIDAD'
        },
        'regic_saude_baixamediaalta_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'DESTINO',
            'namer': 'NOME_ORIGEM'
        },
        'regic_ext_saude_regioes_alta_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'CodReg',
            'namer': 'Nome_Reg'
        },
        'regic_ext_saude_regioes_baixamedia_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'cod_Reg',
            'namer': 'Nome_REG'
        },
        'regic_ext_saude_alta_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'Geocodigo',
            'namer': 'NOME_CIDAD'
        },
        'regic_ext_saude_baixamedia_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'Geocodigo',
            'namer': 'NOME_CIDAD'
        },
        'regic_ext_saude_baixamediaalta_complexidade': {
            'levels': ['macrorregiao', 'mesorregiao', 'microrregiao', 'uf'],
            'identifier': 'DESTINO',
            'namer': 'NOME_ORIGEM'
        },
        'uf': {
            'levels': [
                'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
                'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
                'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade',
                'regic_saude_baixamediaalta_complexidade', 'regic_ext_saude_alta_complexidade',
                'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade'
            ],
            'identifier': 'CD_GEOCUF',
            'namer': 'NM_ESTADO'
        },
        'mesorregiao': {
            'levels': [
                'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
                'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
                'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade',
                'regic_saude_baixamediaalta_complexidade', 'regic_ext_saude_alta_complexidade',
                'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
                'uf'
            ],
            'identifier': 'CD_GEOCME',
            'namer': 'NM_MESO'
        },
        'microrregiao': {
            'levels': [
                'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
                'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
                'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade',
                'regic_saude_baixamediaalta_complexidade', 'regic_ext_saude_alta_complexidade',
                'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
                'mesorregiao', 'uf'
            ],
            'identifier': 'CD_GEOCMI',
            'namer': 'NM_MICRO'
        },
        'municipio': {
            'levels': [
                'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
                'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
                'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade',
                'regic_saude_baixamediaalta_complexidade', 'regic_ext_saude_alta_complexidade',
                'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
                'microrregiao', 'mesorregiao', 'uf'
            ],
            'identifier': 'CD_GEOCMU',
            'namer': 'NM_MUNICIP'
        },
        'distrito': {
            'levels': [
                'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
                'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
                'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade',
                'regic_saude_baixamediaalta_complexidade', 'regic_ext_saude_alta_complexidade',
                'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
                'macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio'
            ],
            'identifier': 'CD_GEOCODD',
            'namer': 'NM_DISTRIT'
        },
        'subdistrito': {
            'levels': [
                'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
                'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
                'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade',
                'regic_saude_baixamediaalta_complexidade', 'regic_ext_saude_alta_complexidade',
                'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
                'macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio', 'distrito'
            ],
            'identifier': 'CD_GEOCODS',
            'namer': 'NM_SUBDIST'
        },
        'setor_censitario': {
            'levels': [
                'regic_saude_regioes_alta_complexidade', 'regic_saude_regioes_baixamedia_complexidade',
                'regic_ext_saude_regioes_alta_complexidade', 'regic_ext_saude_regioes_baixamedia_complexidade',
                'regic_saude_alta_complexidade', 'regic_saude_baixamedia_complexidade',
                'regic_saude_baixamediaalta_complexidade', 'regic_ext_saude_alta_complexidade',
                'regic_ext_saude_baixamedia_complexidade', 'regic_ext_saude_baixamediaalta_complexidade',
                'macrorregiao', 'mesorregiao', 'microrregiao', 'uf', 'municipio', 'distrito', 'subdistrito'
            ],
            'identifier': 'CD_GEOCODI',
            'namer': 'NM_BAIRRO',
            'filters': [
                {
                    'name': 'aglomerados_subnormais',
                    'col_res': 'setor_censitario',
                    'col_filter': 'aglomerados_subnormais'
                }
            ]
        }
    }

    # Correlation columns indicating the belongin of analysis units
    CLUSTER_COLS = {
        'regic_saude_regioes_alta_complexidade': 'cd_alta',
        'regic_saude_regioes_baixamedia_complexidade': 'cd_baixa_media',
        'regic_saude_alta_complexidade': 'cd_alta',
        'regic_saude_baixamedia_complexidade': 'cd_baixa_media',
        'regic_saude_baixamediaalta_complexidade': 'cd_baixa_media',
        'regic_ext_saude_regioes_alta_complexidade': 'cd_alta_ext',
        'regic_ext_saude_regioes_baixamedia_complexidade': 'cd_baixa_media_ext',
        'regic_ext_saude_alta_complexidade': 'cd_alta_ext',
        'regic_ext_saude_baixamedia_complexidade': 'cd_baixa_media_ext',
        'regic_ext_saude_baixamediaalta_complexidade': 'cd_baixa_media_ext',
        'setor_censitario': 'subdistrito'
    }  # Ommited keys are considered to have key == value in the semantics of this script

    def __init__(self, args):
        self.curr_script_dir = "/".join(args[0].split('/')[:-1])
        self.base_dir = "../.."
        self.skip_existing = True

        if len(args) > 1:
            self.base_dir = args[1]
        if len(args) > 2:
            self.skip_existing = args[2]

        self.total_files = 0
        self.total_done = 0
        self.places = self.load_places()

    def update_progress(self, step):
        self.total_done = self.total_done + step
        print(
            f"Converting to geojson: {self.total_done}/{self.total_files} [{int(self.total_done / self.total_files * 100)}%]",
            end="\r",
            flush=True
        )

    def load_places(self):
        """ Load the levels correlations as a pandas dataframe """
        # Granularity = setor_censitario
        print("Generating places correspondence table...", end='\r', flush=True)

        list_municipio = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/municipios').json()
        df = pd.DataFrame.from_dict({
            'municipio': [au.get('id') for au in list_municipio],
            'microrregiao': [au.get('microrregiao', {}).get('id') for au in list_municipio],
            'mesorregiao': [au.get('microrregiao', {}).get('mesorregiao', {}).get('id') for au in list_municipio]
        })
        df = df.assign(
            uf=df['municipio'].astype(str).str.slice(0, 2),
            macrorregiao=df['municipio'].astype(str).str.slice(0, 1)
        )

        # Distritos e subdistritos não apresentam cardinalidade compatível com os municípios (então há lacunas)
        list_temp = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/distritos').json()
        df_temp = pd.DataFrame.from_dict({
            'distrito': [au.get('id') for au in list_temp],
            'municipio': [au.get('municipio', {}).get('id') for au in list_temp]
        })
        df = df.merge(df_temp, on="municipio", how="outer")

        list_temp = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/subdistritos').json()
        df_sub = df_temp.drop(['municipio'], axis=1)
        df_sub = df_sub.assign(
            subdistrito=df_sub['distrito'].astype(str) + '00'
        )
        df_temp = pd.DataFrame.from_dict({
            'subdistrito': [au.get('id') for au in list_temp],
            'distrito': [au.get('distrito', {}).get('id') for au in list_temp]
        })
        df_temp = df_temp.append(df_sub, sort=True)
        df = df.merge(df_temp, on="distrito", how="outer")

        # Evaluate REGIC data as provided by IBGE
        clusters = pd.read_excel(f'{self.curr_script_dir}/REGIC2018_Regionalizacao_Saude_Primeira_Aproximacao.xlsx')
        clusters.columns = ['municipio', 'nm_mun', 'pop18', 'cd_baixa_media', 'nm_baixa_media', 'cd_alta', 'nm_alta']

        df = df.merge(clusters.set_index('municipio'), on="municipio", how="outer")
        df[['cd_baixa_media', 'cd_alta', 'distrito', 'subdistrito']] = df[['cd_baixa_media', 'cd_alta', 'distrito', 'subdistrito']].fillna(0.0).astype(str)

        # Evaluate clusters with all municipalities, including those absent from IBGE's REGIC table
        if os.path.isfile(f'{self.curr_script_dir}/REGIC_melt.csv'):
            clusters_ext = pd.read_csv(f'{self.curr_script_dir}/REGIC_melt.csv')
            clusters_ext = clusters_ext.drop([
                'exerce_influencia_regic', 'influenciado_regic', 'presta_alta', 'presta_baixa', 'procura_servicos_alta',
                'procura_servicos_baixa', 'grau_infl_recebe', 'grau_infl_exerce'
            ], axis=1)
            clusters_ext = clusters_ext.rename(columns={
                "cd_municipio_ibge_dv_origem": "municipio",
                "cd_municipio_ibge_dv_alta_complex": "cd_alta_ext",
                "cd_municipio_ibge_dv_baixa_med_complex": "cd_baixa_media_ext",
            })
            # clusters_ext = clusters_ext.pivot_table(
            # index=['cd_mun_origem'], columns='tp_rel', values='cd_mun_dest', fill_value=0).reset_index()
            # clusters_ext.columns = ['municipio', 'cd_alta_ext', 'cd_baixa_media_ext', 'cd_influencia_ext']
            clusters_ext = clusters_ext.drop_duplicates()
            df = df.merge(clusters_ext.set_index('municipio'), on="municipio", how="outer")
            df[['cd_baixa_media_ext', 'cd_alta_ext']] = df[['cd_baixa_media_ext', 'cd_alta_ext']].fillna(0.0).astype(str)

        df[['cd_baixa_media', 'cd_alta', 'distrito', 'cd_baixa_media_ext', 'cd_alta_ext']] = df[['cd_baixa_media', 'cd_alta', 'distrito', 'cd_baixa_media_ext', 'cd_alta_ext']].replace({'\.0': ''}, regex=True)

        return df

    def get_filtered_places(self, places_id, fltr):
        if fltr.get('name') == 'aglomerados_subnormais':
            df = pd.read_csv(f'{self.curr_script_dir}/AGSN2010Setores.csv')
            df = df.drop(['CD_MUNICIP', 'NM_MUNICIP', 'CD_UF', 'SG_UF'], axis=1)
            # Column renaming: setor_censitario, aglomerado_subnormal_id, aglomerado_subnormal_name
            df.columns = ['join_id', 'place_id', 'name']
            df = df.assign(subdistrito=df['join_id'].astype(str).str.slice(0, 11))
            return df[df['subdistrito'].isin(places_id)].to_dict(orient="records")
        return None

    def make_partition(self, geo_br, f_name, group, identifier, cluster_identifier, fltr=None):
        """ Saving partition """
        os.makedirs(os.path.dirname(f_name), exist_ok=True)

        local_identifier = identifier
        # Falls back to subdistrito, for there's no listing beforehand - it assumes from subdistrito
        if identifier == 'CD_GEOCODI':
            local_identifier = 'CD_GEOCODS'

        if fltr is not None and fltr.get('name') in ['aglomerados_subnormais']:
            local_places = self.get_filtered_places(list(group['subdistrito']), fltr)  # Falls back to setor censitario
            feats = []
            for feature in geo_br.get('features'):
                for each_place in local_places:
                    if feature.get('properties', {}).get(identifier) == str(each_place.get('join_id', '')):
                        nu_feat = feature
                        nu_feat['smartlab_geo_id'] = str(each_place.get('place_id', ''))
                        nu_feat['smartlab_geo_name'] = each_place.get('name')
                        feats.append(nu_feat)
        else:
            list_id = list(group[cluster_identifier].astype(str).unique())
            feats = [
                feature
                for
                feature
                in
                geo_br.get('features')
                if
                feature.get('properties').get(local_identifier) in list_id
            ]

        with open(f_name, "w") as geojson:
            json.dump({"type": "FeatureCollection", "features": feats}, geojson)
        self.update_progress(1)

    @staticmethod
    def read_geometries_from_shapefile(origin):
        reader = shapefile.Reader(origin)
        reader.encoding = "iso-8859-1"
        fields = reader.fields[1:]
        field_names = [field[0] for field in fields]
        buffer = []
        for sr in reader.shapeRecords():
            atr = dict(zip(field_names, sr.record))
            geom = sr.shape.__geo_interface__
            buffer.append(dict(type="Feature", geometry=geom, properties=atr))
        return buffer

    def convert_as_is(self, dataset):
        au_type = dataset.get('au_type')
        # write the GeoJSON files
        if 'file' in dataset:  # Already in BR level
            # Brazil
            if not (self.skip_existing and os.path.isfile(f'{self.base_dir}/geojson/br/{au_type}_q0.json')):
                os.makedirs(os.path.dirname(f'{self.base_dir}/geojson/br/{au_type}_q0.json'), exist_ok=True)
                with open(f'{self.base_dir}/geojson/br/{au_type}_q0.json', "w") as geojson:
                    buffer = self.read_geometries_from_shapefile(
                        f"{self.base_dir}/shapes/{dataset.get('origin')}/{dataset.get('file')}.shp"
                    )
                    for feature in buffer:
                        feature.get('properties')['smartlab_geo_id'] = feature.get('properties').get(
                            dataset.get('identifier')
                        )
                        feature.get('properties')['smartlab_geo_name'] = feature.get('properties').get(
                            dataset.get('namer')
                        )
                    json.dump({"type": "FeatureCollection", "features": buffer}, geojson)
            self.update_progress(1)
        else:  # All the rest is in UF level - generate as it is and then join the features to a single, BR, geojson
            buffer = []
            # UF (iterate)
            for root, dirs, files in os.walk(f"{self.base_dir}/shapes/{dataset.get('origin')}"):
                # path = root.replace(f"{self.base_dir}/geojson", "")
                for file in files:
                    if file.endswith(".shp"):
                        au_id = file.replace('.shp', '')
                        local_buffer = self.read_geometries_from_shapefile(
                            f"{self.base_dir}/shapes/{dataset.get('origin')}/{au_id}.shp"
                        )
                        for feature in local_buffer:
                            feature.get('properties')['smartlab_geo_id'] = feature.get('properties').get(
                                dataset.get('identifier')
                            )
                        buffer.extend(local_buffer)
                        if not (self.skip_existing and os.path.isfile(f'{self.base_dir}/geojson/br/uf/{au_type}/{au_id}_q0.json')):
                            os.makedirs(
                                os.path.dirname(f'{self.base_dir}/geojson/br/uf/{au_type}/{au_id}_q0.json'),
                                exist_ok=True
                            )
                            with open(f'{self.base_dir}/geojson/br/uf/{au_type}/{au_id}_q0.json', "w") as geojson:
                                json.dump({"type": "FeatureCollection", "features": local_buffer}, geojson)
                        self.update_progress(1)
            # Brazil
            if not (self.skip_existing and os.path.isfile(f'{self.base_dir}/geojson/br/{au_type}_q0.json')):
                with open(f'{self.base_dir}/geojson/br/{au_type}_q0.json', "w") as geojson:
                    json.dump({"type": "FeatureCollection", "features": buffer}, geojson)
            self.update_progress(1)
        return

    def generate(self, res_id, level, identifier, fltr=None):
        """ Generates new topologies by combining levels and resolutions """
        # read the BR geojson
        if os.path.isfile(f"{self.base_dir}/geojson/br/{res_id}_q0.json"):
            f_name = f"{self.base_dir}/geojson/br/{res_id}_q0.json"
        else:
            return
        with open(f_name, 'r') as json_file:
            geo = json.load(json_file)

        col = self.CLUSTER_COLS.get(level, level)
        col_res = self.CLUSTER_COLS.get(res_id, res_id)

        local_places = self.places.copy()
        if fltr is not None and res_id != 'setor_censitario':
            local_places = local_places[local_places[fltr.get('col_res')] == local_places[fltr.get('col_filter')]]

        grouped = local_places.groupby(col)
        self.total_files = self.total_files + len(grouped)
        self.update_progress(0)
        for id_part, part in grouped:
            if fltr is None:
                f_name = f'{self.base_dir}/geojson/br/{level}/{res_id}/{id_part}_q0.json'
            else:
                f_name = f"{self.base_dir}/geojson/br/{level}/{res_id}/{fltr.get('name')}/{id_part}_q0.json"
            if self.skip_existing and os.path.isfile(f_name):
                continue
            # Filter geometries and save
            self.make_partition(geo, f_name, part, identifier, col_res, fltr)
        return

    def run(self):
        print("Starting conversion to geojson...", end='\r', flush=True)

        self.total_files = self.total_files + len(self.DATASETS) + 1
        with multiprocess.Pool(processes=4) as pool:
            pool.map(self.convert_as_is, self.DATASETS)

        # Generate combinations levels x geometries
        pool_combinations = []  # Pool to address combination of levels and resolutions of geographies
        for res_id, res in self.RESOLUTIONS.items():
            # Iterate over levels to filter the resolution geometries
            for level in res.get('levels'):
                pool_combinations.append((res_id, level, res.get('identifier')))
        with multiprocess.Pool(processes=4) as pool:
            pool.starmap(self.generate, pool_combinations)

        # Generate filters
        pool_filters = []  # Pool to address combination of levels and resolutions of geographies
        for res_id, res in {r_id: r for r_id, r in self.RESOLUTIONS.items() if 'filters' in r}.items():
            # Iterate over levels to filter the resolution geometries
            for fltr in res.get('filters'):
                for level in res.get('levels'):
                    pool_filters.append((res_id, level, res.get('identifier'), fltr))
        with multiprocess.Pool(processes=4) as pool:
            pool.starmap(self.generate, pool_filters)

        print(f"All shapes converted to geojson!!!!")


# Run the code
Shape2Geo(sys.argv).run()
