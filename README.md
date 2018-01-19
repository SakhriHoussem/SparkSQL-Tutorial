# Spark SQL in Practice

##

### Buid a DataFrames :

#### Buid a DataFrames from text files :
create class for each text file

```scala
// create class for text file state
case class state(name : String, number : Int)
```

```scala
// create class for text file participants
case class participant(ParticipantID : Int, name : String, city : String, age : Int)
```

```scala
// create class for text file exits
case class exits(HikingID : Int, ParticipantID : Int,dateExite : String, durationExit : String)
```

import data from each text files to Dataframes :

```scala
// import data from state to Dataframes :
val S = sc.textFile("file:<folder path>/input/state.txt").map(_.split(",")).map(p => state(p(0),p(2).toInt)).toDF()
```

```scala
// import data from participants to Dataframes :
val P = sc.textFile("file:<folder path>/input/participants.txt").map(_.split(",")).map(p => participant(p(0).toInt,p(1),p(2),p(3).toInt)).toDF()
```

```scala
// import data from exits to Dataframes :
val E= sc.textFile("file:<folder path>/input/exits.txt").map(_.split(",")).map(p => exits(p(0).toInt,p(1).toInt,p(2),p(3))).toDF()
```

#### Buid a DataFrames from Json file :

```scala
val H = sqlContext.read.json("file:<folder path>/input/hikings.json")
```

#### show DataFrames content : 

show stats DataFrames content

```
// show stats DataFrames content
S.show()
```
or
```scala
// create temporary table
S.registerTempTable("state")
// show stats DataFrames content
val result = sqlContext.sql("SELECT * FROM state")
```

show Hikings DataFrames content
```
// show Hikings DataFrames content
H.show()
```
or
```scala
// create temporary table
H.registerTempTable("hikings")
// show stats DataFrames content
val result = sqlContext.sql("SELECT * FROM hikings")
```

show participants DataFrames content

```
// show participants DataFrames content
P.show()
```
or
```scala
// create temporary table
P.registerTempTable("participants")
// show stats DataFrames content
val result = sqlContext.sql("SELECT * FROM participants")
```

show exits DataFrames content

```
// show exits DataFrames content
E.show()
```
or
```scala
// create temporary table
E.registerTempTable("exits")
// show stats DataFrames content
val result = sqlContext.sql("SELECT * FROM exits")
```

### Queries :

#### Show the distance of the hike from "Murjadju Mountain" :
```scala
H.filter($"name" === "Montagne de Murdjadju").select( $"distance").show
```	
or
```scala
// create temporary table
H.registerTempTable("hikings")
//
val result = sqlContext.sql("SELECT distance FROM hikings where name='Montagne de Murdjadju'")
```

#### Show distances by region :

```scala
H.select( $"distance",$"region").groupBy("region").sum().show()
```
ou
```scala
H.registerTempTable("hikings")
//
val result = sqlContext.sql("SELECT SUM(distance), region FROM hikings GROUP BY region").show 
```

#### Show participants aged between 35 and 50 years old :
```scala
P.filter($"age">= 35).filter($"age"<= 50).show()
```
or
```scala
// create temporary table
P.registerTempTable("participants")
//
val result = sqlContext.sql("SELECT * FROM participants where age BETWEEN 35 AND 50 ").show
```

#### Show the names of the hikes and the number of the state corresponding to the regions : 

```scala
val result  = S.join(r, w.col("name") === H.col("region"),"inner").select(H.$"name", S.$"number").show()
```	
or
```scala
// create temporary tables
H.registerTempTable("hikings")
S.registerTempTable("state")
P.registerTempTable("Participants")
//
val result = sqlContext.sql("SELECT hikings.name, state.number FROM hikings,state WHERE hikings.region = state.name").show()
```

#### Show participants who hiked more than 1000 meters of altitude.

```scala
val result  = E.join(H, E.col("HikingID") === H.col("HikingID"),"inner").join(P, E.col("ParticipantID") === P.col("ParticipantID"),"inner").filter($"altitude" > 1000).select(H.$"name").show()
```
or
```scala
// create temporary tables
H.registerTempTable("hikings")
P.registerTempTable("Participants")
E.registerTempTable("exits")
//
val result = sqlContext.sql("SELECT Participants.name FROM hikings,Participants,exits WHERE exits.HikingID = hikings.HikingID AND  exits.ParticipantID = Participants.ParticipantID AND hikings.altitude>1000 ").show
```
