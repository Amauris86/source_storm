# Twitter Streaming con Apache Storm

Esta aplicación permite clasificar un tweet como Positivo(1), Negativo(-1) o Neutro(0), el proyecto define una topologia Apache Storm en la cual se clasifica el tweet, una vez definido el sentimiento del tweet el resultado sera almacenado en una base de datos MySQL y en el filesystem del Sistema Operativo. ver [video](http://youtu.be/y_khI3KRI_A) en el cual se describe paso a paso el despliegue de la aplicación.    

## Requisitos

Los servicios necesarios para la ejecución de la topologia son ejecutados en una maquina virtual [Hortonworks Sandbox with HDP 2.2](http://hortonworks.com/hdp/downloads/). En esta maquina se ejecuta:

* Apache Storm 0.9.3
* Apache Ambari 1.7.0
* MySQL 5.1.73
* Java 1.7.0_71
* CentOS 6.6

El sourcecode que define la topologia utiliza diferentes APIs:

* [API Twitter4J 4.0.2](http://twitter4j.org/en/index.html#download) que permite obtener los tweets.
* [Apache Storm Core 0.9.3](https://storm.apache.org/downloads.html)
* [MySQL Connectors Java 5.0.8](https://dev.mysql.com/downloads/connector/)

El proyecto usa [Apache Maven](https://maven.apache.org/download.cgi) 2.2.1 para gestionar las dependencias necesarias. 
El proyecto fue desarrollado usando [Eclipse Java EE IDE](https://eclipse.org/downloads/) 4.4.1.

## Keys y Access Token de Twitter

Para obtener los Keys y Access Token es necesario crear una aplicación en [Twitter](https://apps.twitter.com/). Una vez creada es necesario editar la clase "TwitterSpout" indicando los Keys y Access Tokens generados por la aplicación creada en Twitter.

``` java
cb.setOAuthConsumerKey("eU9ZQCb4OrloQglOc6UTbERcm").
		setOAuthConsumerSecret("Qk6y8S39XMtxbjLrCwyBGdHZxyc3Gr5d0EqSW1OGx2mP37mtC2").
		setOAuthAccessToken("85721956-ea8eXE5H6NjPvpAPh2UsxfWsAVmdbrXhgXXCNJpCu").
		setOAuthAccessTokenSecret("OEbHgHkJxg4qWP8HmR7E5E5cPrRND1bhzbVb6hhhyHvfX");
```

## Keywords hacer seguimiento

También es necesario definir las palabras clave que desea hacer seguimiento en los Tweets en la clase "TwitterSpout".

``` java
tweetFilterQuery.track(new String[] {"MovistarCo", "ClaroColombia", "Tigo_Colombia",
					"UNEMejorjuntos", "Virgin_MobileCo", "Uff_MOVIL"});
```

## Definir conexión mysql

Editar la clase "MySQLDB" indicando la dirección IP de la maquina en la cual se encuentra el servidor de base de datos MySQL. Editar el nombre de la base de datos, usuario y password si se uso uno diferente al definido en el [video](http://youtu.be/y_khI3KRI_A).

``` java
con = DriverManager.getConnection("jdbc:mysql://192.168.0.13/db_twitter_storm", 
									"twitter", "twitter");
```

## Generar el paquete jar

Generar el archivo .jar que contenga las dependencias de la aplicación usando maven.

```
mvn install
```

Maven creará el directorio "target" en el workspace del proyecto en el cual se encuentra el archivo .jar a ser desplegado en Apache Storm, usar el archivo con dependencias.
