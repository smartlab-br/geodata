import requests
import json
import os
import sys
import math
import multiprocess
from ftptool import FTPHost
from io import BytesIO
import zipfile

base_url_au = 'https://servicodados.ibge.gov.br/api/v1/localidades'

# 0. Nenhuma divisão político-administrativa é inserida no interior da malha
# 1. Inclui na malha as macrorregiões. Válido apenas quando a localidade for BR.
# 2. Inclui na malha as Unidades da Federação. Válido apenas quando a localidade for BR ou uma macroregião
# 3. Inclui na malha as mesorregiões. Válido apenas quando a localidade for BR, macroregião ou Unidade da Federação
# 4. Inclui na malha as microrregiões. Válido apenas quando a localidade for BR, macroregião, Unidade da Federação ou mesorregião
# 5. Inclui na malha os municípios
resolution = ['br', 'macrorregiao', 'uf', 'mesorregiao', 'microrregiao', 'municipio']

ua_url = {
    'macrorregiao': 'regioes',
    'uf': 'estados',
    'mesorregiao': 'mesorregioes',
    'microrregiao': 'microrregioes',
    'municipio': 'municipios'
}

def select_destination(main, det, quality, id_au):
    base="../../geojson/"
    if main == det:
        if main == 'br':
            return f'{base}/br/_q{quality}.json'
        return f'{base}/br/{main}/{id_au}_q{quality}.json'
    elif main == 'br':    
        return f'{base}/br/{det}_q{quality}.json'
    return f'{base}/br/{main}/{det}/{id_au}_q{quality}.json'

def download_file(key, value, resolution, id_au, skip_existing):
    global total_files, total_done
    base_url = 'https://servicodados.ibge.gov.br/api/v2/malhas'

    # Padrão de qualidade da imagem. Pode assumir valores de 1 a 4, sendo 1 o de qualidade mais inferior. Por padrão, assume o valor 4
    qualities = list(range(1,5))

    # Formato de renderização da malha. Útil quando o usuário preferir informar o formato de renderização diretamente na URL do navegador, sem a necessidade de informar o parâmetro Accept
    fmt = 'application/vnd.geo+json'

    for key_detail, value_detail in enumerate(resolution):
        for quality in qualities:
            f_name = select_destination(value, value_detail, quality, id_au)
            if skip_existing and os.path.isfile(f_name):
                total_done = total_done + 1
                print(f"Downloading: {total_done}/{total_files} [{int(total_done/total_files*100)}%]    ", end="\r", flush=True)
                continue
            os.makedirs(os.path.dirname(f_name), exist_ok=True)

            location = f'{base_url}/{id_au}?resolucao={key + key_detail}&qualidade={quality}&formato={fmt}'
            # print(f'Downloading from: {location}')
            r = requests.get(location, headers={'Accept': fmt, 'Content-Type': fmt})
            # print(f'Generating: {f_name}')
            with open(f_name, 'w', encoding='utf-8') as f:
                json.dump(r.json(), f)
                # f.close() # Just to make sure it releases memory
            total_done = total_done + 1
            print(f"Downloading: {total_done}/{total_files} [{int(total_done/total_files*100)}%]    ", end="\r", flush=True)

def download_and_unzip(dirname, zip_file_name, dest, unit=None):
    global total_files, total_done
    # Download the .zip
    ftp = FTPHost.connect("geoftp.ibge.gov.br", user="anonymous", password="anonymous@")
    ftp.current_directory = "/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2010/setores_censitarios_shp"
    f = ftp.file_proxy(f"{dirname}/{zip_file_name}")
    fp = BytesIO()
    f.download(fp)

    # Unzip it, renaming the target
    zip_file = zipfile.ZipFile(fp)
    for name in zip_file.namelist():
        ext = name.split('.')[-1]
        # zip_file.extract(name, f"{dest}/{unit}.{ext}")
        with zip_file.open(name) as internal_file:
            if unit is not None:
                f_name = f"{dest}/{unit}.{ext}"
            else:
                f_name = f"{dest}.{ext}"
            if skip_existing and os.path.isfile(f_name):
                total_done = total_done + 1
                print(f"Downloading and unzipping: {total_done}/{total_files} [{int(total_done/total_files*100)}%]    ", end="\r", flush=True)
                continue
            os.makedirs(os.path.dirname(f_name), exist_ok=True)
            with open(f_name, "wb") as target_file:
                target_file.write(internal_file.read())
                total_done = total_done + 1
                print(f"Downloading and unzipping: {total_done}/{total_files} [{int(total_done/total_files*100)}%]    ", end="\r", flush=True)
    return

# print(f"Starting topologies download...", end="\r", flush=True)
# if sys.argv[1] is None:
#     skip_existing = True
# else:
#     skip_existing = sys.argv[1].lower() in ['true', '1', 't', 'y', 'yes']

# total_files = 0
# total_done = 0

# for key, value in enumerate(resolution):
#     if value == 'br':
#         total_files = total_files + 1
#         download_file(key, value, resolution[key:], '', skip_existing)
#     else:
#         r_au = requests.get(f'{base_url_au}/{ua_url[value]}')
#         list_au = r_au.json()
#         total_files = total_files + len(list_au) * len(resolution[key:]) * 4
#         f_name = f'analysis_units_{value}.json'
#         with open(f_name, 'w', encoding='utf-8') as f:
#             json.dump(r_au.json(), f)
#             # f.close() # Just to make sure it releases memory
#         for au in list_au:
#             # print(f'Changing analysis unit to {au.get("id")}')
#             download_file(key, value, resolution[key:], au.get("id"), skip_existing)

print(f"Starting shapefiles download...", end="\r", flush=True)
total_files = total_files + 4 * 3 * 27

base_dest = '../../shapes/territorio'
with open('analysis_units_uf.json') as json_file:
    uf_sigla2cod = {uf.get('sigla').lower():uf.get('id') for uf in json.load(json_file)}

# Download and unzip topologies from IBGE FTP service
ftp = FTPHost.connect("geoftp.ibge.gov.br", user="anonymous", password="anonymous@")
pool_args = []
ftp.current_directory = "/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2010/setores_censitarios_shp"
for (dirname, subdirs, files) in ftp.walk('.'):
    for zip_file_name in files:
        # Load only the zip files, except the municipalities
        if '.zip' in zip_file_name and 'municipios' not in zip_file_name:
            file_name = zip_file_name.replace(".zip","")
            unit = str(uf_sigla2cod.get(file_name.split('_')[0]))
            resolution = "".join("_".join(file_name.split('_')[1:]).split())

            # Build target directory
            dest = f"{base_dest}/uf/{resolution}"
            pool_args.append((dirname, zip_file_name, dest, unit))

with multiprocess.Pool(processes=8) as pool:
    pool.starmap(download_and_unzip, pool_args)

# Download and unzip topologies from IBGE FTP service (municipalities, micro-regions, meso-regions states and regions)
ftp = FTPHost.connect("geoftp.ibge.gov.br", user="anonymous", password="anonymous@")
pool_args = []
ftp.current_directory = "/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2015/Brasil/BR/"
for (dirname, subdirs, files) in ftp.walk('.'):
    for zip_file_name in files:
        # Load only the zip files, except the BR.zip
        if '.zip' in zip_file_name and 'BR' not in zip_file_name:
            file_name = zip_file_name.replace(".zip","")
            resolution = "".join("_".join(file_name.split('_')[1:]).split())

            # Build target directory
            dest = f"{base_dest}/{resolution}"
            pool_args.append((dirname, zip_file_name, dest))

with multiprocess.Pool(processes=8) as pool:
    pool.starmap(download_and_unzip, pool_args)

print(f"All files downloaded and unzipped!!!!       ")