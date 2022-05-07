from datetime import datetime, timedelta
import pendulum
import prefect
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import pandas as pd
from io import BytesIO
import os
import sqlite3
import requests

# Criando as configurações do agendamento
schedule = IntervalSchedule(interval=timedelta(minutes=2))

# Criando uma função para extrair cada um dos arquivos csv de suas URLS
def download_url(url,name):
	req = requests.get(url)
	url_content = req.content
	csv_file = open(name, 'wb')
	csv_file.write(url_content)
	csv_file.close()

# Criando a task que cria as pastas de destino
@task
def create_folders():
	try:
		os.stat('./bronze_layer')
	except:
		os.mkdir('./bronze_layer')
	try:
		os.stat('./silver_layer')
	except:
		os.mkdir('./silver_layer')
	try:
		os.stat('./gold_layer')
	except:
		os.mkdir('./gold_layer')
	return True

# Criando a task que puxa os dados da web
@task
def get_raw_data(status):
	if status:
		# Definindo as urls
		city_url = 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/WorldDBTables/CityTable.csv'
		country_url = 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/WorldDBTables/CountryTable.csv'
		language_url = 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/WorldDBTables/LanguageTable.csv'

		# Baixando os arquivos na sua forma crua
		download_url(url=city_url,name='./bronze_layer/city_raw.csv')
		download_url(url=country_url,name='./bronze_layer/country_raw.csv')
		download_url(url=language_url,name='./bronze_layer/language_raw.csv')

	return True

# Criando uma task de pré-processamento do dataset city
@task
def city_preprocessing(status):
	if status:
		# Importando dados
		city = pd.read_csv('./bronze_layer/city_raw.csv')
		
		# Removendo dados desnecessários
		city = city[city['country_code']!='UMI']

	return city

# Criando uma task de pré-processamento do dataset country
@task
def country_preprocessing(status):
	if status:
		# Importando dados
		country = pd.read_csv('./bronze_layer/country_raw.csv')

		# Removendo dados desnecessários
		country = country[country['capital']!='NuLL']

		# Transformando os dados do códigos dos países para letras maiúsculas
		country['code'] = country['code'].str.upper()

	return country

# Criando uma task de pré-processamento do dataset language
@task
def language_preprocessing(status):
	if status:
		# Importando dados
		language= pd.read_csv('./bronze_layer/language_raw.csv')

		# Removendo dados desnecessários
		language = language[language['country_code']!='UMI']

	return language

# Criando uma task para adicionar a coluna de porcentagem da população para o dataset de cidades
@task
def constroi_population_perc(silver_city,silver_country):
	city = silver_city
	country = silver_country
	city2 = pd.merge(city,
					 country[['code','population']],
					 left_on = 'country_code',
					 right_on = 'code')
	city2['population_perc'] = city2['population_x']/city2['population_y']*100
	city2.drop(['population_y','code'],
			   axis = 1,
			   inplace = True)
	city2.rename({'population_x':'population'},
				 axis = 1,
				 inplace = 1)
	
	return city2[['population_perc']]

# Criando uma task para adicionar a coluna de número de linguagens para o dataset de países
@task
def constroi_lang_num(silver_country,silver_language):
	language = silver_language
	country = silver_country
	n_lang = language['country_code'].value_counts()
	dict_lang = dict(
		zip(
			n_lang.index,
			n_lang.values)
		)
	cnt_mapped = country['code'].map(dict_lang)
	cnt_mapped.fillna(1,inplace=True)
	cnt_mapped = cnt_mapped.astype('int64')
	
	return pd.DataFrame({'lang_num':cnt_mapped})

# Criando uma task para adicionar a coluna de número de falantes da lingua de cada a país
@task
def constroi_speaker(silver_country,silver_language):
	language = silver_language
	country = silver_country
	speakers_dict = dict(
		zip(
			country['code'],
			country['population'])
		)
	lang_speakers = language['country_code'].map(speakers_dict)
	lang_speakers = language['percentage']*lang_speakers

	return pd.DataFrame({'lang_speakers':lang_speakers})

# Criando uma task para preencher a coluna old_gnp dos países faltantes baseados na média do old_gnp de suas regiões
@task
def preenche_gnp_old(silver_country):
	country = silver_country
	# Filtrando os dados de país para aqueles em que os valores da coluna gnp_old estão corretos
	country2 = country[country['gnp_old']!='NuLL'].copy()
	# Transformando a coluna de gnp_old que antes eram strings em numéricos
	country2.gnp_old = country2.gnp_old.astype('int64')
	# Criando uma variavel que agrupa por regioes as médias dos gnp_old
	old_gnp_mean = country2.groupby('region').mean().round().reset_index()
	# Criando um dicionario que ira conter como chave as regioes e como valores as médias dos seus gnp_old
	OGM_dict = dict(zip(old_gnp_mean['region'],old_gnp_mean['gnp_old']))
	# Criando valores adiconais no dicionario para as regioes que nao possuem nenhum valor de gnp_old
	OGM_dict['Antarctica']=0
	OGM_dict['Micronesia/Caribbean']=0
	# Criando uma lista com os valores já mapeados utilizando o dicionario criado anteriormente
	OGM_list = country.loc[country['gnp_old']=='NuLL','region'].map(OGM_dict)
	# Atribuindo aos valores de gnp_old que são iguais a NuLL a lista criada anteriormente
	country.loc[country['gnp_old']=='NuLL','gnp_old'] = OGM_list
	# Mudando o tipo de dado
	country['gnp_old'] = country['gnp_old'].astype('int64')
	
	# Exportando a coluna
	return country[['gnp_old']]

# Criando uma task para criar uma variável que diz se o gnp foi melhorado ou não
@task
def better_gnp(preenche_gnp_old,silver_country):
	real_old = preenche_gnp_old
	country2  = silver_country
	better_gnp = country2['gnp'] > real_old['gnp_old']

	return pd.DataFrame({'better_gnp':better_gnp})

@task
def join_city(silver_city,constroi_population_perc):
	city 		= silver_city
	ct_pop_perc	= constroi_population_perc
	
	city_final	  = pd.concat([city,ct_pop_perc],axis=1)
	
	city_final.to_csv('./silver_layer/city_final.csv',index=False)
	
	return city_final

@task
def join_country(silver_country,constroi_lang_num,better_gnp):
	country 	= silver_country
	gnp 		= better_gnp
	lang_num	= constroi_lang_num
	
	country_final = pd.concat([country,gnp,lang_num],axis=1)
	
	country_final.to_csv('./silver_layer/country_final.csv',index=False)
	
	return country_final

@task
def join_language(silver_language,constroi_speaker):
	language = silver_language
	speakers = constroi_speaker

	language_final = pd.concat([language,speakers],axis=1)

	language_final.to_csv('./silver_layer/language_final.csv',index=False)

	return language_final

# Criando uma task que escreve em um banco de dados SQL as tabelas finais criadas anteriormente
@task
def write_dw(final_city,final_country,final_language):
	city_final = final_city
	country_final = final_country
	language_final = final_language
	path = './gold_layer/world.db'
	conn = sqlite3.connect(path)
	c = conn.cursor()
	# Tabela das cidades
	c.execute('''CREATE TABLE IF NOT EXISTS city (id number,
	                                              name text,
	                                              country_code text,
	                                              district text,
	                                              population number,
	                                              population_perc number
	                                              )''')
	conn.commit()

	# Tabela dos países
	c.execute('''CREATE TABLE IF NOT EXISTS country (code text,
	                                                 name text,
	                                                 continent text,
	                                                 region text,
	                                                 surface_area number,
	                                                 independence_year number,
	                                                 population number,
	                                                 life_expectancy number,
	                                                 gnp number,
	                                                 gnp_old number,
	                                                 local_name text,
	                                                 government_form text,
	                                                 head_of_state text,
	                                                 capital number,
	                                                 code2 text,
	                                                 better_gnp text,
	                                                 lang_num number
	                                                 )''')
	conn.commit()

	# Tabela de línguagens
	c.execute('''CREATE TABLE IF NOT EXISTS language (country_code text,
	                                                  language text,
	                                                  official text,
	                                                  percentage number,
	                                                  speakers number
	                                                  )''')
	conn.commit()
	city_final.to_sql('city',conn,if_exists='replace',index=False)
	country_final.to_sql('country',conn,if_exists='replace',index=False)
	language_final.to_sql('language',conn,if_exists='replace',index=False)

with Flow('WorldDB',schedule=schedule) as flow:
	status_folder = create_folders()
	status_download = get_raw_data(status_folder)
	silver_city = city_preprocessing(status_download)
	silver_country = country_preprocessing(status_download)
	silver_language = language_preprocessing(status_download)
	constroi_population_perc = constroi_population_perc(silver_city,silver_country)
	constroi_lang_num = constroi_lang_num(silver_country,silver_language)
	constroi_speaker = constroi_speaker(silver_country,silver_language)
	preenche_gnp_old = preenche_gnp_old(silver_country)
	better_gnp = better_gnp(preenche_gnp_old,silver_country)
	final_city = join_city(silver_city,constroi_population_perc)
	final_country = join_country(silver_country,constroi_lang_num,better_gnp)
	final_language = join_language(silver_language,constroi_speaker)
	write_dw(final_city,final_country,final_language)

flow.register(project_name='Learning Projects',
			  idempotency_key=flow.serialized_hash())
flow.run_agent()