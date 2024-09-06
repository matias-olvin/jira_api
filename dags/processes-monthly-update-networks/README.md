# NETWORKS V2

Diagram
on https://lucid.app/lucidchart/767a54c7-f1a8-43ba-9673-fe2105db6835/edit?page=0_0&invitationId=inv_694722ca-dacd-42d1-bb28-aa62892847d1#

Trigger with

``
{ "result_date_trigger": "20XX-XX-01" }
``

## Visits vs Visitors

If to a POI 1 visitor visits once and another visitor 3 times. If the visits factor is 100, and that means 400 visits,
it also means 200 visitors.

Also a connection/journey between A and B by one person, means the connection of A to B by 100 people.

## 1. INPUT LINK PREDICTION

1. Full visits - Visits to POIs and HOMEs. We remove visits close to home. Each visit has assigned a visit_score_real.
   The maximum visit to a place of a device represents the `visitor`
2. Connection Combination - Each device's edge for being at POI_A and POI_B. There are `n * (n-1) / 2` combinations of
   POI_x POI_y as no repeated. Edge got by multiplying the `visitor` value
3. Journey Combinations

## 2. LINK PREDICTION (SHORT DESCRIPTION)

For creation of neo4j database the links and nodes are extracted from bigquery and stored in the bucket. From bucket
then the data is loaded in the VM instance and its where the ML pipeline for link prediction is created.

Date preparation:

Each node in the neo4j database represents a POI. The GDS library of neo4j database has a many algorithms available for
node classification, topological link prediction, centrality calculation, machine learning models of link prediction.
For this update the machine learning model of link prediction is used. The undirected graph model is implemented for
link prediction which uses the Logistic regression classification model as underlying model of classification. For
features normalised latitude, normalised logitude, visit times, income, age group are used as input features.

Note : The input features should be normaised or standardised before fetching in the databse.

1. Format of edges and nodes for neo4j database

    1. Create the nodes.csv.gzip file, nodes_header.csv file, edges_header.csv, edges.csv.gzip file in follwowing format
       only.

    2. Format of edges.csv header: :START_ID,weight:float,:END_ID,:TYPE

    3. Fomat for the edges.csv.gzip: 23p-222@5sb-88z-z75 10.634586681500000 zzw-22h@5sb-88z-vpv RELATION

    4. Format for nodes_header.csv: fk_sgplaces:ID,fk_sgplaces:string,latitude:double,longitude:double .....brand:int

    5. Format of nodes.csv.gzip file: 222-223@8g9-m9w-5j9, -0.20507695993840100, 1.3150908045971100, 76, ...... 1

2. Import the nodes.csv.gzip, edges.csv.gzip, nodes_header.csv, edges_header.csv into vm instance by running sh script
   using the ssh airflow operator into a folder(we name this folder month) at the location /usr/bin/month/.

Steps 3 to 5 are already saved in the VM insatnce. Each cypher file has corresponding .sh shell script. Using the shell
scripts the cypher queries are executed from airflow ssh operator. Note - Do not use the same database name for next
month update.

#   

(See detailed description below for steps 3-6)

3. Run the neo4j-admin command (from /usr/bin/ folder only) for loading the database using the files above by stopping
   the neo4j database service. Note - Using the neo4j-admin command may throw permission issues while running any cypher
   queries in future. So, to overcome this issue one can either set the umask in the config file on the vm instance or
   use the commnad 'sudo chmod -R 777 /var/lib/neo4j/data/'

4. Turn on the neo4j service. Create the database in system mode and then start the database in system mode (wait for 5
   mins before starting the newly created database as it takes time for the new database to be available for use).

5. Create graph using graph.cypher. Create ML pipeline using pipe.cypher. Train graph using train.cypher. The prediction
   is done in mutate mode using prediction.cypher. Once the links are predicted then the predcited edges are saved using
   save.cypher. The neo4j_id are saved using the neo4j_id.cypher.

6. After the links are predicted the relationships_predicted are saved back to bucket. The links are loaded in bigquery
   dataset sg_networks in table predicted_links and neo4j_id in table neo4j_id.

7. Predicted links inner join neo4j_id to get fk_sgplaces. Filter chains from predcited links.
8. Transform the probability to weights using the transformation.sql and save as estimated table
9. Save to postgres as last table to be sent to database

To push to remote branch(not master) from your local branch: git push origin sg_networks_v2

# LINK PREDICTION

(Detailed description of the files created in VM insatnce)
Before running the dag turn on Vm insatance and check the conditions mentioned in section 1 and its subsection 1-4 We
create a new user database everytime data is loaded in neo4j. The steps for the working with neo4j are described below.

1. SECTTION 1 : Create Neo4j database:

Before creating the database make sure that there are only 2 database that exist, ie . neo4j and system. This can be
checked in cypher shell as follows:

0. ssh the local machine:
   `gcloud compute ssh name_of_the_instance`


1. Accessing Cypher shell through browser(pass the internal ip address of the vm instance):
   > http://34.76.19.70:7474/browser/

   Note - The internal ip address of the vm instance get changed when the machine is turned off/ vm instance is stopped.

2. Accessing Cypher shell through terminal through system mode or neo4j default database mode:
   The cypher-shell can only be accessed through /usr/bin/ on ubuntu machine, as the cypher-shell executable file is
   stored here. Here p is the password and d is the name of default database
   > /usr/bin$ cypher-shell -u neo4j -a neo4j://35.195.240.165:7687 -p oct -d neo4j
3. Checking if there are any existing databases cypher-shell> SHOW DATABASES; If the above command displays only 2
   databases ie. system and neo4j, then we can proceed to create a new database. If the above command show more than 2
   database then the third database can be deleted using command:
   cypher-shell> DROP DATABASE name_of_database_to_be_deleted;

4. Check the database folder of the /var/lib/neo4j/data/databases/ for any extra folder. It should only contain (neo4j,
   store_lock, system )

The Vm instance is now ready for creating the database. The steps and queries are explained below in detail. Each
important step query is written in a different file as to avoid any failure and restarting the process from start.

2. SECTION 2 :

   In the link_prediction.py each ssh airflow operator corresponds to a shell script on the VM insatnce.
   # 1.
   load_data.sh :
   This file loads the data from bucket to instance. In this file the name of database is also gives which is set in
   other files.

   # 2. Run
   run1.sh

   This file executes neo4j-admin command to create database from scratch. This is the most fastest way of loading data.
   This command can only be executed after the database serivce of neo4j is stopped. After running this command from
   root user there might be permission issues while running other cypher-shell queries. So, either use umask in config
   file or use chmod 777 after every cypher-shell query.

   Do not use :
   (CALL apoc.periodic.iterate('CALL apoc.load.jdbc("jdbc:bigquery:
   //https://www.googleapis.com/bigquery/v2:443;ProjectId=olvin-sandbox-20210203-0azu4n;OAuthType=0;OAuthServiceAcctEmail=neo4j-179@olvin-sandbox-20210203-0azu4n.iam.gserviceaccount.com;OAuthPvtKeyPath=/home/sakshi/olvin-sandbox-20210203-0azu4n-4b56922a5097.json","SELECT*
   From `olvin-sandbox-20210203-0azu4n.networks_training.35_met_nodes` ",[], {autoCommit:true}) YIELD row','CREATE (n:
   Node {nodeid: toString(row.fk_sgplaces), nodename:toString(row.fk_sgplaces),latitude:toFloat(row.latitude_trf_std)
   ,longitude:toFLoat(row.longitude_trf_std)...})',{ batchSize:10000, parallel:true});)

# Run

run2.sh create the database

# Run

run3.sh start the database

# Run

run4.sh set the label on node

# Run

run5.sh create the graph from GDS library

CALL gds.graph.create(
'POI', { Node: { label: 'Node', properties:{ latitude:{ property:'latitude' }, longitude:{ property:'longitude' },
visits_ln:{ property:'visits_ln' }, fk_life_stages_1:
{ property:'fk_life_stages_1' },

fk_life_stages_2:
{ property:'fk_life_stages_2' },

fk_life_stages_3:
{ property:'fk_life_stages_3' },

fk_life_stages_4:
{ property:'fk_life_stages_4' },

fk_life_stages_5:
{ property:'fk_life_stages_5' }, fk_incomes_1:{ property:'fk_incomes_1' }, fk_incomes_2:{ property:'fk_incomes_2' },
fk_incomes_3:{ property:'fk_incomes_3' }, fk_incomes_4:{ property:'fk_incomes_4' }, fk_incomes_5:{ property:'
fk_incomes_5' } } } }, { RELATION: { orientation: "UNDIRECTED", aggregation: "DEFAULT", type: "RELATION", properties: {
weight:{ property:'weight' } } } });

# Run

run6.sh Create the ML pipeline CALL gds.alpha.ml.pipeline.linkPrediction.create('latlong_pipe');

# Run

run7.sh Upadate the ML pipeline by adding features, embeddings, preparing the test and train data by splitting the edges

CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('latlong_pipe', 'l2', {
nodeProperties: ['latitude','longitude','.....']
}) YIELD featureSteps;

#   

The embedding are not added for this update. However, in future it can be addded like this CALL
gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('latlong_pipe', 'fastRP', { mutateProperty: '
fastRP_Extended_Embedding', featureProperties: ['fk_life_stages_1', '.....','fk_incomes_5'], embeddingDimension: 2,
relationshipWeightProperty: 'weight', iterationWeights: [0.7, 0.5, 1.0, 1.0], normalizationStrength:0.05, randomSeed: 42
});

CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('latlong_pipe', { testFraction: 0.3, trainFraction: 0.3,
negativeSamplingRatio:1.5, validationFolds: 7 })
YIELD splitConfig;

CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('latlong_pipe',
[{tolerance: 0.001}, {tolerance: 0.01}, {batchSize:5000},{penalty: 0.5, maxEpochs: 1500}, {penalty: 1.0, maxEpochs: 1500}, {penalty: 0.0, maxEpochs: 1500}]
) YIELD parameterSpace;

# Run

run8.sh Train the model of the ML pipeline. Concurrency can be set to max value of 4 on free enterprise version of
library. CALL gds.alpha.ml.pipeline.linkPrediction.train('POI', { pipeline: 'latlong_pipe', modelName: '
lat_linkpredict_with_embedding', concurrency:4, randomSeed: 42 }) YIELD modelInfo RETURN modelInfo.bestParameters AS
winningModel, modelInfo.metrics.AUCPR.outerTrain AS trainGraphScore, modelInfo.metrics.AUCPR.test AS testGraphScore;

# Run

run9.sh Predict the link of the ML pipeline in mutate mode. This query predicts top k for every node. So, incase we
predict top 100 for every node, the number of edges predicted will be 100*2*(number of nodes). As every new edge
increases degree of graph by 2. Always predict the links in the mutate mode as its the fastest way of generating the
results. The parameters randomjoins, samplerate can be set to lower values as to speed up the process. The parameter
deltaThreshold : 0.12 corresponds to min value of probability for edges to be considered(greatest lower bound on
probabity of edges).

CALL gds.alpha.ml.pipeline.linkPrediction.predict.mutate('POI', { modelName: 'lat_linkpredict_with_embedding',
relationshipTypes: ['RELATION'], mutateRelationshipType: 'RELATION_PREDICTED', sampleRate: 0.3, topK: 100, randomJoins:
2, maxIterations: 3, deltaThreshold : 0.12, concurrency: 1, randomSeed: 42 }) YIELD relationshipsWritten, samplingStats;

#   

Save the results of the predictions run10.sh Predict the link of the ML pipeline in mutate mode.

CALL gds.beta.graph.export.csv('POI', { exportName: 'month'});

#   

This query saves the IDs of teh neo4j database and corresponding fk_sgplaces With "MATCH (n) WHERE EXISTS(n.fk_sgplaces)
RETURN DISTINCT id(n) as ID, n.fk_sgplaces AS fk_sgplaces" AS query1 CALL apoc.export.csv.query(query1, "neo4j_id.csv",
{})YIELD file,rows, batchSize, batches, done RETURN file,rows, batchSize, batches, done;

#   

Export the neo4j_id and predicted_edges to the bucket
`sudo gsutil -m cp -r gs://neo4j_demo/current/historical_edges_*.csv.gzip /var/lib/neo4j/import/export/month/"relationships_relation_predicted_"*.csv`

`sudo gsutil -m cp -r gs://neo4j_demo/current/neo4j_id /var/lib/neo4j/import/neo4j_id.csv`

#   

3. POSTGRES FORMATING

After the results are loaded in the bucket.

1. The predicted edges and nodes_id are loaded from bucket to bigquery
2. The edges are filtered using chaintochain task which results in selecting the edges which are now only chains. Inner
   join between noe4j_id and predcited edges results in fk_sgplaces of src_node and dst_node
3. Tranformation is applied on the predicted_chaintochain_table
4. estimated_connections_table is then converted to json format.


4. Helpful tips/ Usual errors

- to make a `.sh` executable --> `sudo chmod +x load_data.sh`
  chmod +x on a file (your script) only means, that you'll make it executable. Right click on your script and chose
  Properties -> Permissions -> Allow executing file as program, leaves you with the exact same result as the command in
  terminal.
- There should no be spaces when defining the variable month:
  month=placeholder -- RIGHT month =placeholder -- ERROR