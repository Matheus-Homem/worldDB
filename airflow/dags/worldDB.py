# Importação de Bibliotecas
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import zipfile
import sqlite3

# Argumentos default
default_args = {
	'owner':'Matheus - IGTI',
	'depends_on_past':False,
	'start_date':datetime(2022,5,1,),
	'email':['matheuschomem@hotmail.com'],
	'email_on_failure':False,
	'email_on_retry':False,
	'retries':1,
	'retry_delay':timedelta(minutes=1)
}

# Vamos definir a DAG - Fluxo
dag = DAG(
	'world-db',
	description='Performa um ETL nos dados relacionados ao dataset World_DB_Tables',
	default_args=default_args,
	schedule_interval=None
)

# Obtendo os datasets:

commands = """
	cd airflow;
	mkdir -p data;
	cd data;
	mkdir  bronze_layer;
	mkdir  silver_layer;
	mkdir  gold_layer;
	curl https://raw.githubusercontent.com/datasciencedojo/datasets/master/WorldDBTables/CityTable.csv -o ~/airflow/data/bronze_layer/city.csv;
	curl https://raw.githubusercontent.com/datasciencedojo/datasets/master/WorldDBTables/CountryTable.csv -o ~/airflow/data/bronze_layer/country.csv;
	curl https://raw.githubusercontent.com/datasciencedojo/datasets/master/WorldDBTables/LanguageTable.csv -o ~/airflow/data/bronze_layer/language.csv;
	"""

get_data = BashOperator(
	task_id='get_data',
	bash_command=commands,
	dag=dag
)

#===========================================================================================#

# Criando as funções em Python:

def preprocessing():
	# Importing data
	country 	= pd.read_csv('~/airflow/data/bronze_layer/country.csv')
	city 		= pd.read_csv('~/airflow/data/bronze_layer/city.csv')
	language 	= pd.read_csv('~/airflow/data/bronze_layer/language.csv')
	
	# City Dataset
	
	# Country Dataset
	country = country[country['capital']!='NuLL']
	country['code'] = country['code'].str.upper()
	
	# Languange Dataset
	language = language[language['country_code'] != 'UMI']

	# Exporting data
	country.to_csv('~/airflow/data/silver_layer/country.csv',index=False)
	city.to_csv('~/airflow/data/silver_layer/city.csv',index=False)
	language.to_csv('~/airflow/data/silver_layer/language.csv',index=False)


# Função para adicionar a coluna de porcentagem da população no dataset de cidades
def constroi_population_perc():
	city = pd.read_csv('~/airflow/data/bronze_layer/city.csv')
	country = pd.read_csv('~/airflow/data/bronze_layer/country.csv')
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
	city2[['population_perc']].to_csv('~/airflow/data/silver_layer/ct_pop_perc.csv',index=False)

# Função para adicionar a coluna de número de linguagens no dataset de países
def constroi_lang_num():
	language = pd.read_csv('~/airflow/data/silver_layer/city.csv')
	country = pd.read_csv('~/airflow/data/silver_layer/country.csv')
	n_lang = language['country_code'].value_counts()
	dict_lang = dict(
		zip(
			n_lang.index,
			n_lang.values)
		)
	country['lang_num'] = country['code'].map(dict_lang)
	country['lang_num'].fillna(1,inplace=True)
	country['lang_num'] = country['lang_num'].astype('int64')
	country[['lang_num']].to_csv('~/airflow/data/silver_layer/cnt_lang_num.csv',index=False)

# Função para adicionar a coluna de número de falantes da lingua de cada a país
def constroi_speakers():
	language = pd.read_csv('~/airflow/data/bronze_layer/language.csv')
	country = pd.read_csv('~/airflow/data/bronze_layer/country.csv')
	speakers_dict = dict(
		zip(
			country['code'],
			country['population'])
		)
	language['speakers'] = language['country_code'].map(speakers_dict)
	language['speakers'] = language['percentage']*language['speakers']
	language[['speakers']].to_csv('~/airflow/data/silver_layer/lang_speakers.csv',index=False)

# Função para preencher a coluna old_gnp dos países faltantes baseados na média do old_gnp de suas regiões
def preenche_gnp_old():
	country = pd.read_csv('~/airflow/data/silver_layer/country.csv',usecols=['region','gnp_old'])
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
	country[['gnp_old']].to_csv('~/airflow/data/silver_layer/gnp_old_filled.csv',index=False)

# Função para criar uma variável que diz se o gnp foi melhorado ou não
def melhor_gnp():
	real_old = pd.read_csv('~/airflow/data/silver_layer/gnp_old_filled.csv')
	country  = pd.read_csv('~/airflow/data/silver_layer/country.csv')
	country['melhor_gnp'] = country['gnp'] > real_old['gnp_old']
	country[['melhor_gnp']].to_csv('~/airflow/data/silver_layer/cnt_melhor_gnp.csv',index=False)

# Função que junta todas as colunas criadas anteriormente
def join_data():
	country 	= pd.read_csv('~/airflow/data/silver_layer/country.csv')
	city 		= pd.read_csv('~/airflow/data/silver_layer/city.csv')
	language 	= pd.read_csv('~/airflow/data/silver_layer/language.csv')
	ct_pop_perc	= pd.read_csv('~/airflow/data/silver_layer/ct_pop_perc.csv')
	melhor_gnp 	= pd.read_csv('~/airflow/data/silver_layer/cnt_melhor_gnp.csv')
	lang_num	= pd.read_csv('~/airflow/data/silver_layer/cnt_lang_num.csv')
	speakers 	= pd.read_csv('~/airflow/data/silver_layer/lang_speakers.csv')

	country_final = pd.concat([country,melhor_gnp,lang_num],axis=1)
	city_final	  = pd.concat([city,ct_pop_perc],axis=1)
	language_final= pd.concat([language,speakers],axis=1)

	country_final.to_csv('~/airflow/data/silver_layer/country_final.csv',index=False)
	city_final.to_csv('~/airflow/data/silver_layer/city_final.csv',index=False)
	language_final.to_csv('~/airflow/data/silver_layer/language_final.csv',index=False)

def escreve_dw():
	country_final = pd.read_csv('~/airflow/data/silver_layer/country_final.csv')
	city_final = pd.read_csv('~/airflow/data/silver_layer/city_final.csv')
	language_final = pd.read_csv('~/airflow/data/silver_layer/language_final.csv')
	path = './airflow/data/gold_layer/world.db'
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
	                                                 melhor_gnp text,
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

#==========================================================================================#

# Criando os Operadores em Python:

# Operador para filtrar linhas e concertar valores dentro dos próprios datasets originais
task_preprocessing = PythonOperator(
	task_id='task_preprocessing',
	python_callable=preprocessing,
	dag=dag
)

# Operador para adicionar a culuna de porcentagem na população no dataset de cidades
task_pop_perc = PythonOperator(
	task_id='task_pop_perc',
	python_callable=constroi_population_perc,
	dag=dag
)

# Operador para adicionar a coluna de número de linaguens no dataset de países
task_lang_num = PythonOperator(
	task_id='task_lang_num',
	python_callable=constroi_lang_num,
	dag=dag
)

# Operador para adicionar a coluna de número de falantes da lingua de cada a país
task_speakers = PythonOperator(
	task_id='task_speakers',
	python_callable=constroi_speakers,
	dag=dag
)

# Operador para preencher a coluna old_gnp dos países faltantes baseados na média do old_gnp de suas regiões
task_preenche_gnp_old = PythonOperator(
	task_id='task_preenche_gnp_old',
	python_callable=preenche_gnp_old,
	dag=dag
)

# Operador para criar uma variável que diz se o gnp foi melhorado ou não
task_melhor_gnp = PythonOperator(
	task_id='task_melhor_gnp',
	python_callable=melhor_gnp,
	dag=dag
)

task_join = PythonOperator(
	task_id='task_join',
	python_callable=join_data,
	dag=dag
)

task_escreve_dw = PythonOperator(
	task_id='escreve_dw',
	python_callable=escreve_dw,
	dag=dag
)

#===========================================================================================#

get_data >> task_preprocessing

task_preprocessing >> [task_pop_perc,task_lang_num,task_speakers,task_preenche_gnp_old]

task_preenche_gnp_old >> task_melhor_gnp

task_join.set_upstream([task_pop_perc,task_lang_num,task_speakers,task_melhor_gnp])

task_join >> task_escreve_dw