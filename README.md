# Campanha Nacional de Vacinação contra Covid-19 | Semantix

- Projeto de conclusão de curso utilizando dados da "[Campanha Nacional de Vacinação contra Covid-19](https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar)"
- Referência: [Painel Geral](https://covid.saude.gov.br/) do site Coronavítus Brasil.


<details>
<summary>Veja como foram os passos para criação desse projeto</summary>

#### 1. Enviar os dados para o hdfs
> Primeiro subi os containers Docker com `docker compose up` e acessei o container namenode.
>
> Criei o diretório no HDFS

````
hdfs dfs -mkdir -p /user/spark/projeto_final_basico 
````

>  Transferi os arquivos do Linux para o diretório HDFS criado.
 
 ````
 hdfs dfs -put /input/HIST_PAINEL_COVIDBR_2021_Parte1_06jul2021.csv /user/spark/projeto_final_basico
 ````
 
 ````
 hdfs dfs -put /input/HIST_PAINEL_COVIDBR_2021_Parte2_06jul2021.csv /user/spark/projeto_final_basico
 ````

#### 2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município.
> Dentro do Spark importei as blibliotecas e criei a SparkSession

````
import pyspark as spark
from pyspark.sql.functions import *

spark = SparkSession\
.builder\
.appName('Projeto final Básico - Campanha Nacional de Vacinação contra Covid-19')\
.config('spark.some.config.option', 'some-value')\
.enableHiveSupport()\
.getOrCreate()
````
> Li o arquivo csv e salvei no Dataframe **csv_df**

 ````
 csv_df = spark.read.csv('hdfs://namenode/user/spark/projeto_final_basico', sep=";",header=True, inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)
 ````
 
````
csv_df.show(10, vertical=True)
````
 
> Visualizei o Schema, alterei o campo data para melhor visualização e verifiquei 30 linhas para entender melhor.

````
csv_df_to_unix = csv_df.withColumn('data', from_unixtime(unix_timestamp(df.data), 'dd-MM-yyyy'))
````

````
csv_df.printSchema()
````

````
csv_df_to_unix.show(30, vertical=True)
````

> Criei o Banco de dados "covid"

````
spark.sql("create database covid")
````

> Particionei por municipio

````
csv_df_to_unix.write.mode('overwrite').partitionBy('municipio').format('csv').saveAsTable('covid.municipio', path='hdfs://namenode:8020/user/hive/warehouse/covid_municipio/')
````

> Aquela verificação para ver se está tudo correto

````
!hdfs dfs -ls /user/hive/warehouse/covid_municipio
````

> Visualizei os bancos de dados existentes, selecionei o BD covid e visualizei as tabelas existentes ( até então somente a municipio )

````
spark.sql("SHOW DATABASES").show()
````

````
spark.sql("USE covid")
````

````
spark.sql("SHOW TABLES").show()
````

> Visualizei 200 linhas da tabela municipio para melhor compreensão

````
spark.sql("SELECT * FROM municipio").show(200,vertical=True)
````

#### 3. Criar as 3 vizualizações pelo Spark com os dados enviados para o HDFS

##### Como explicado no PDF os valores mostrados eram somente uma referẽncia, então deixei minha curiosidade rolar e criei visualizações únicas.

> Primeria visualização criada fiz em duas partes. ( As visualizações estão abaixo README )
>
> * 1 Visualização: Obitos estados do maior para o menor -> 1º Semestre 2021 (01/01/2021 - 30/06/2021)

````
estado_obitos = spark.sql("SELECT estado, MAX(obitosAcumulado) AS obitos FROM municipio WHERE estado IS NOT NULL GROUP BY estado ORDER BY obitos DESC")
````

````
estado_obitos.show()
````

> * 1.1 Visualização: Óbitos pelas regiões do Brasil e Brasil como um todo. ( Brasil inicia o ano de 2021 com 195.411 óbitos )

````
regiao_br_obitos = spark.sql("select regiao, max(obitosAcumulado) as obitos from municipio group by regiao order by obitos desc")
````

````
regiao_br_obitos.show()
````

> * 2 Visualização: Número total de casos novos no fim do primeiro semestre de 2021.

````
casos_novos = spark.sql("SELECT estado, sum(casosNovos) AS casos_novos FROM municipio where estado IS NOT NULL group by estado order by casos_novos desc")
````

````
casos_novos.show()
````

> * 3 Visualização: Valor médio de casos novos e óbitos diários no primeiro semestre por estado.

````
casos_obitos_media = spark.sql("SELECT estado, ROUND(SUM(casosNovos) / COUNT(data),2) AS media_casos_novos , ROUND(AVG(obitosAcumulado),2) AS media_obitos_diarios FROM municipio WHERE estado IS NOT NULL GROUP BY estado ORDER BY media_casos_novos DESC")
````

````
casos_obitos_media.show()
````

#### 4. Salvar a primeira visualização como tabela Hive

> Salvei a 1º como tabela HIVE

````
estado_obitos.write.format('csv').saveAsTable('Obitos_por_estado')
````

````
regiao_br_obitos.write.format('csv').saveAsTable('Obitos_por_regiao')
````

> Visualizei se foram salvas corretamente

````
spark.sql('SHOW TABLES').show()
````

#### 5. Salvar a segunda visualização com formato parquet e compressão snappy

> Salvei com formato parquet e compressão snappy

````
casos_novos.write.option('compression', 'snappy').parquet('/user/spark/projeto_final_basico/segunda_visualizacao')
````

> Conferindo se foi salvo corretamente

````
!hdfs dfs -ls '/user/spark/projeto_final_basico/segunda_visualizacao'
````

#### 6. Salvar a terceira visualização em um tópico no Kafka

> Converti para JSON e salvei em um tópico kafka.

````
casos_obitos_media.selectExpr("to_json(struct(*)) AS value").write.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('topic', 'casos_obitos_media').save()
````

````
topic = spark.read.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092').option('subscribe','casos_obitos_media').load()
````

````
topic_media_casos_obitos = topic.select(col('value').cast('string'))
````

````
topic_media_casos_obitos.show(truncate = False)
````

#### 7. Criar a visualização pelo Spark com os dados enviados para o HDFS:

> Criei uma visualização geral no Spark com todos os dados enviados para o HDFS : Síntese de casos, óbitos, incidência e mortalidade

````
df_geral = csv_df.groupBy(['regiao', 'estado']).agg({'casosAcumulado':'max', 'obitosAcumulado':'max', 'populacaoTCU2019':'max'})
````

````
df_renomear_campos = df_geral.withColumnRenamed('max(populacaoTCU2019)','populacao').withColumnRenamed('max(casosAcumulado)', 'casos_acumulados').withColumnRenamed('max(obitosAcumulado)','obitos_acumulados')
````

````
df_geral_completo = (df_renomear_campos.withColumn('incidencia', round(df_renomear_campos['casos_acumulados']/df_renomear_campos['populacao']*100000,1)).withColumn('mortalidade', round(df_renomear_campos['obitos_acumulados']/df_renomear_campos['populacao']*100000,1)))
````

> Visualizei minha criação!

````
df_geral_completo.show(10)
````

#### 8. Salvar a visualização do exercício 6 em um tópico no Elastic

````
df_final = topic_media_casos_obitos
````

````
df_final.write.format("csv").save('hdfs://namenode/user/spark/projeto_final_basico/visualizacao3/covid_br.csv')
````

> Houve um problema :
> POR ALGUM MOTIVO QUE DESCONHEÇO E DEPOIS DE MUITA PESQUISA E DIVERSAS TENTATIVAS NÃO COMPREENDI O MOTIVO DO PORQUE O ARQUIVO "covid_br.csv" NÃO FOI SALVO EM MINHA MÁQUINA LOCALMENTE..IMPOSSIBILITANDO-ME DE FINALIZAR O PROJETO..
>
>Professor Rodrigo espero que considere todo o projeto até aqui, realmente localmente o arquivo não existe mesmo constando essa mensagem ao dar run na célula "!hdfs dfs -get /user/spark/projeto_final_basico/visualizacao3/covid_br.csv /input"
>
>A mensagem que aparece é : 
>get: `/input/covid_br.csv/_SUCCESS': File exists`
>
>get: `/input/covid_br.csv/part-00000-a5470f49-ccb0-44fa-befd-7e89afd283f7-c000.csv': File exists`
>
>Porém não há nada no diretório..
>
>Obrigado.

~9. Criar um dashboard no Elastic para visualização dos novos dados enviados~

> **Foi utilizado dados da campanha de vacição do COVID-19 onde foi feito ingestão dos dados no HDFS, depois os dados foram lidos com PySpark usando Jupyter Notebook, criação de DataFrames e suas operações, escrita das tabelas no Hive, Kafka e no Elastic e visualização de dashboards criada no Kibana.**

</details>


