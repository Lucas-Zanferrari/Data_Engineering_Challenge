# Desafio de engenharia de dados da Semantix

## Perguntas teóricas

### Qual o objetivo do comando cache​ ​em Spark?

Ele faz com que o resultado de uma action executada sobre um RDD permaneça salvo na memória após o fim de sua execução. 
Isso é útil para evitar que actions sejam reprocessadas (e portanto, ganhar performance) sem necessidade conforme o 
script efetua consultas e transformations sobre o RDD.



### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?

Pois ao utilizar o Spark, a computação acontece em memória (exceto se ela não couber na memória). 
Os MapReduce jobs, em comparação, envolvem diversas operações de input e output do disco rígido por padrão.



### Qual é a função do SparkContext​?

O SparkContext é o objeto principal do framework. É através dele que todas as funcionalidades nativas do Spark são acessadas.



### Explique com suas palavras o que é Resilient​ ​Distributed​ ​Datasets​ (RDD).

RDD é uma coleção de elementos distribuída paralelamente pelo cluster (mais comumente um cluster Hadoop).
Dois tipos de operação podem ser efetuadas sobre o RDD: transformations e actions.
Transformations não possuem valor de retorno e só são computadas quando a próxima action ocorre (isso é 
chamado de lazy evaluation). Actions possuem valor de retorno e são os gatilhos das transformations. 
Um exemplo (em Python) segue abaixo:
```
# .filter() é uma transformation. Nada ocorre após esta declaração
trasformation = rdd.filter(lambda column1: column1 > 10)

# .count() é a primeira action após a transformation, logo, ela é responsável por executar o procedimento registrado
# pela transformation e, após isso, computar o seu próprio resultado sobre o RDD
print(transformation.count())
```


### GroupByKey​ ​é menos eficiente que reduceByKey​ ​em grandes dataset. Por quê?

Devido a forma que ambas as funções trabalham, o GroupByKey tende a requerer que volumes de dados muito maiores 
sejam enviados pela rede do cluster durante a sua execução. Isso se deve ao fato de que a função reduceByKey é capaz
de reduzir (ou agrupar) previamente (em cada partição) elementos do RDD que possuem a mesma chave, assim apenas o resultado
dessa redução prévia é enviado pela rede. No caso do GroupByKey, todos os elementos de cada partição são enviados
individualmente pela rede para que a redução ocorra depois, de forma que a rede pode ser sobrecarregada e a performance
da computação, debilitada.



### Explique o que o código Scala abaixo faz.

```
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
```

A primeira instrução lê o conteúdo de algum arquivo de texto para um RDD atribuído ao valor (imutável) textFile. 
A segunda registra uma série de transformations que devem ser executadas quando a próxima action sobre
textFile ocorrer, sendo elas:  

- `.flatMap(line => line.split(" "))`  
	Computa a lista de palavras no arquivo de texto. Uma palavra é representada por qualquer sequência de caracteres
	que esteja entre, antes ou após de um caractere de espaço (como definido pela função line.split(" "))
- `.map(word => (word, 1))`  
	Computa uma lista de tuplas com base na lista de palavras supracitada. Cada tupla da lista possui a palavra como 
	primeiro elemento e o número 1 como segundo
- `.reduceByKey(_ + _)`  
	Reduz a lista de tuplas da transformação anterior, de forma que palavras duplicadas sejam removidas e 
	a segunda posição de suas tuplas contenham a onúmero de ocorrências destas respectivas palavras
Esta sequência de transformations é atribuída a um valor chamado counts.  
  
A terceira executa a action .saveAsTextFile() sobre o valor counts, ou seja, ela dispara a sequência de 
transformations e, quando a computação termina, salva o resultado em um arquivo de texto novo (cujo
caminho é passado como parâmetro da função).
