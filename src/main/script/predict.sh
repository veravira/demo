#!/bin/bash

### THIS SCRIPT loads the file of SAMPLE transaction ids (stored in answers-sample.csv) and creates predicted price for each ###
### Aggregates the results into answers.csv ###
### Passes answers.csv to be joined with initial set of hws2.csv to display the predicted prices ###


echo "testing our sample - price predictor"  

cat A-*/part-r-00000 >> answers-sample.csv

echo "################# File of aggregated predictions is ready ############## "
pig -x local -f aggregator.pig -param filename=answers-sample.csv