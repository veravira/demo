#!/bin/bash

### THIS SCRIPT loads the file of all transaction ids and creates predicted price for each ###
### Aggregates the results into answers.csv ###
### Passes answers.csv to be joined with initial set of hws2.csv to display the predicted prices ###

echo "testing our sample - price predictor"  
pig -x local extract-distinct-transId.pig
echo "unique-tds folder is created"

for LINE in `cat unique-tds/part-r-00000`
do
echo "finding similar cars to car  " $LINE
pig -x local -f pricePredictor.pig -param transId=$LINE

echo "################# Results ############## "

done
cat A-*/part-r-00000 >> answers.csv

echo "################# File of aggregated predictions is ready ############## "
pig -x local -f aggregator.pig -param filename=answers.csv