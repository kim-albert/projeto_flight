# projeto_flight
Projeto criado para estudos utilizando Shell e Hive.
<br>
**Ambiente:** imagem cloudera 5.13.x

> Deve ser criado uma nova estrutura no Hive sempre que for executado e pipeline.

**Entrada:** 3 arquivos CSV.
<br>
**Saida:** 7 views respondendo a perguntas especificas com base nos arquivos CSV.

---
### Estrutura dos diretorios
**Arquivos csv**
<br>
home/cloudera/workspace/mount_cloudera/

**Arquivos sql**
<br>
home/cloudera/desafio_flight/script/sql

**Arquivos sh**
<br>
home/cloudera/desafio_flight/script/sh

**HDFS**
<br>
/db_flight/**raw**/tb_airport
<br>
/db_flight/**raw**/tb_airline
<br>
/db_flight/**raw**/tb_aflights
<br>
/db_flight/**transform**/tb_aeroporto_companhia
<br>
/db_flight/**transform**/tb_voo
