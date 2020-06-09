# geodata
Geodata used in smartlab products.

The purpose of this repository is to document and maximize the automation of steps to retrieve geographic data, slice and combine the levels and resolutions in order to get topologies files as small as possible with the minimum loss of visual details.

Shapefiles and geojson will not be versioned here due to file size limitations and we'll do our best to add final topojson files to the repository.

Bear in mind that we do not provide the shapefiles, so it's possible that the geometries hold inconsistencies. In such cases, we recommend (as we do it ourselves) you fix the downloaded shapefiles and run the script again, skipping the download step - it will generate geojsons and topojsons.

# Processing time
Conversion to topojson is time-consuming. To speed up the process, we run multiple threads (default 8). In the near future, you'll be able to customize it.

# Geographical data sources:
## Territorial shapes:
Source: IBGE (FTP)

The script is programmed to download the shapes from IBGE FTP service.

## Territorial datasets:
Source: IBGE (API Localidades) - https://servicodados.ibge.gov.br/api/docs/localidades?versao=1

## REGIC shapes and dataset:
Source: IBGE

Downloaded from: https://www.ibge.gov.br/geociencias/cartas-e-mapas/redes-geograficas/15798-regioes-de-influencia-das-cidades.html?=&t=downloads

Downloaded in Apr 28th, 2020.
