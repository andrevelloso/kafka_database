# kafka_to_database
kafka stream to database oracle with PDI

Para testar kafka para um stream de dados, precisará

1.kafka Broker   - Pode utilizar um serviço em Docker 

2.kafka Producer - Projeto anexo em Maven 

3.kafka Consumer - Projeto anexo em Pentaho Data Integration versão 9.4

Requisitos
1. Java Virtual Machine instalado - Ex.OpenJDK 11
2. Maven - Utilizamos Apache Maven 3.8.8
3. Pentaho Data Integration versão 9.4 (referido sempre como PDI)
4. Base de dados. Utilizamos Oracle 19c (lembre de colocar o driver JDBC na pasta ..\data-integration\lib)
5. Editor de codigo: recomendo Visual Studio Code

Na base de dados, deve criar a tabela para receber os dados, neste exemplo, poderá usar:

CREATE TABLE kafka_test_1(
  nome varchar2(50),
  cidade varchar2(50),
  empresa varchar2(50),
  ts TIMESTAMP NOT NULL
 );

No PDI, criar no ficheiro de transformação "tra_kafka_consumer_child" uma conexão a base de dados (pode utilizar o wizard).

   
