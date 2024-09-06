## VISITS SHARE

This pipeline consists on preparing the data to train the visits share model.

This pipeline has to be run just when improvements on the model are done or when the `poi_visits` data has changed.


All the development code can be found here: https://github.com/agarciadecorral/visits_share

### PIPELINE STRUCTURE

This model has two parts which are considered independant so they can be trained and used in parallel.

#### PART 1: Classifier
1. Places Filtering: Based on a few thresholds we create a list of places which without overlapping with some neighbour-places, they are really close. We will keep the category(in a next iteration the POI representation) for validation purposes.

2. Training Data RAWGoing to the `poi_visits`, select a range of dqates and filtering visits to only the places we are interested in.

3. Final Dataframe - selecting the needed columns for training purposes undergoing a proper data processing. We also join the category of the neighbourhood for validation purposes.



#### PART 2: Probability update based on proximity with centroids. (prior_distance)
1. Distance to Centroid table - Creation of a table with distances to centroids by met area.


## TO DO
- In next iteration, change the regressors using the new methodology - DONE
- Input shouldn't be preprocessed in BQ (i.e. local_hour(or categories) should be a number from 0 to 23 and the tensorflow class should deal with it(see quality or real_visits models)) - DONE

- RERUN ONCE WEATHER/ HOLIDAYS IS DONE FOR ALL US



## IMPLEMENTATION

### VERTEX AI

Once the data is ready, we use VERTEX AI to:
- Train and validate the classifier model and distance_to_centroid distribution  - go to https://github.com/agarciadecorral/visits_share and follow the README.md
- Once the model is created it will be sent to GCS and then to BQ


### TO BE DONE IN THE VISITS PIPELINE

- The model will be saved on BQ
- The distribution of distances function will be stored (TBD)
