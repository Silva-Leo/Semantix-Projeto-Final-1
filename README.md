# Campanha Nacional de Vacinação contra Covid-19 | Semantix

- Projeto de conclusão de curso utilizando dados da "[Campanha Nacional de Vacinação contra Covid-19](https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar)"
- Referência: [Painel Geral](https://covid.saude.gov.br/) do site Coronavítus Brasil.


<details>
<summary>Veja como foram os passos para criação desse projeto</summary>

> 1. Enviar os dados para o hdfs
>
> 2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município.
>
> 3. Criar as 3 vizualizações pelo Spark com os dados enviados para o HDFS:
>
> 4. Salvar a primeira visualização como tabela Hive
>
> 5. Salvar a segunda visualização com formato parquet e compressão snappy
>
> 6. Salvar a terceira visualização em um tópico no Kafka
>
> 7. Criar a visualização pelo Spark com os dados enviados para o HDFS:
>
> 8. Salvar a visualização do exercício 6 em um tópico no Elastic
>
> 9. Criar um dashboard no Elastic para visualização dos novos dados enviados

> **Foi utilizado dados da campanha de vacição do COVID-19 onde foi feito ingestão dos dados no HDFS, depois os dados foram lidos com PySpark usando Jupyter Notebook, criação de DataFrames e suas operações, escrita das tabelas no Hive, Kafka e no Elastic e visualização de dashboards criada no Kibana.**

</details>


