import requests
import json
import os

base_url = 'https://servicodados.ibge.gov.br/api/v2/malhas'
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

# Padrão de qualidade da imagem. Pode assumir valores de 1 a 4, sendo 1 o de qualidade mais inferior. Por padrão, assume o valor 4
qualities = list(range(1,5))

# Formato de renderização da malha. Útil quando o usuário preferir informar o formato de renderização diretamente na URL do navegador, sem a necessidade de informar o parâmetro Accept
fmt = 'application/vnd.geo+json'

def select_destination(main, det, quality, id_au):
    if main == det:
        if main == 'br':
            return f'br/_q{quality}.json'
        return f'br/{main}/{id_au}_q{quality}.json'
    elif main == 'br':    
        return f'br/{det}_q{quality}.json'
    return f'../../geojson/br/{main}/{det}/{id_au}_q{quality}.json'

def download_file(key, value, resolution, id_au, skip_existing):
    for key_detail, value_detail in enumerate(resolution):
        for quality in qualities:
            f_name = select_destination(value, value_detail, quality, id_au)
            if skip_existing and os.path.isfile(f_name):
                continue
            os.makedirs(os.path.dirname(f_name), exist_ok=True)

            location = f'{base_url}/{id_au}?resolucao={key + key_detail}&qualidade={quality}&formato={fmt}'
            # print(f'Downloading from: {location}')
            r = requests.get(location, headers={'Accept': fmt, 'Content-Type': fmt})
            # print(f'Generating: {f_name}')
            with open(f_name, 'w', encoding='utf-8') as f:
                json.dump(r.json(), f)
                f.close()
    
def run(skip_existing=False):
    for key, value in enumerate(resolution):
        if value == 'br':
            download_file(key, value, resolution[key:], '', skip_existing)
        else:
            r_au = requests.get(f'{base_url_au}/{ua_url[value]}')
            list_au = r_au.json()
            f_name = f'analysis_units_{value}.json'
            with open(f_name, 'w', encoding='utf-8') as f:
                json.dump(r_au.json(), f)
                f.close()
            for au in list_au:
                # print(f'Changing analysis unit to {au.get("id")}')
                download_file(key, value, resolution[key:], au.get("id"), skip_existing)

run(True)