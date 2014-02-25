HW
======
## Instructions ##
Clone the repository:
``` 	git clone https://github.com/veravira/demo.git ```

Build the jar
```	mvn clean package ```

### To Recommend 5 similar cars ###
To find 5 similar trim ids for the given trim_id 
Run the following 
``` 	pig -x local cluster.pig ```
that will create grp-joined folder.

Copy that folder into file  
``` cp grp-joined/part-r-00000 cl-price-body-type.csv ``` 
and run 
``` pig -x local recommender.pig ```

### To Predict Prices ###

Run the following 
``` 	pig -x local extract-distinct-transId.pig ```
that will create file of distinct transaction ids (folder unique-tds would be created).

run 
``` ./test_predictor.sh ```
but it may be slow

to run predictor on the sample set, please run the following script
``` ./predict.sh ```

## Assignment # 1 ##
1. Building a price prediction model
2. Related files: extract-distinct-transId.pig, pricePredictor.pig, aggregator.pig, test_predictor.sh
3. The idea was to observe and learn how the price column is related to any other information given in "hw set 1".
4. Formulate the model (price model). 
	*	We need to test the model on "hw set 2" to make the predictions.
	*	Normalize the data by removing any duplicates. 
	*	Create clusters (groups) by the (year, model_id, door, transmission)
I created a macro (predictPrice(grp, record)) that predict the price based on the following formula: 
		*	Observe the average price of base MSRP value   
		*	Observe the average price of transaction MSRP value
		*	subtract b from a and dived by 2 and add that to the base MSRP, this would be the predicted price 
  
Note1: perhaps model_id already contains the door and transmission information so maybe no need to group by them (door and transmission, they may be already involved in the model_id) 

Note2: I had some problems parsing the engine column and trim column, so I removed them and worked with the file without them due to 
time constrains.

Remark 1: Please don't remove  "A-*/part-r-00000" folders and files, since they are used in running small test to predict prices! 
Remark 2: Run predict.sh to predict prices on the small data set (sample)
Remark 3: To predict prices on the complete hw2 set please run test_predictor.sh.
Remark 3.a: test_predictor.sh may take some time for script to complete (needs optimization!)
Remark 3.b: run predict.sh to predict prices on the smaller set
Remark 4: Do NOT remove tds/part-r-00000 (this file contains distinct transaction ids from hw set 2)

Note 3: Predict Price by adding to the base msrp a difference in (transaction_msrp-base_msrp ) * 0.5
Note 4: Use cash_dealer, cash_customer in price prediction as well as lease, finance bit.
____________________________________


cp grp-joined/part-r-00000 cl-price-body-type.csv
1) Cluster are built with cluster.pig
Sample data is stored in ../sample/src/main/script/cl-price-body-type.csv
it's TAB delimitered.

2) shell script and pig macro based on trim_id returns 5 similar cars
*	returns the whole cluster 
*	compute the price difference
*	returns only 5 cars that have the least differences  

# Notes/ideas use datafu UDF from linkedin #

## Assignment # 2
## Building a recommender system ##

Related files:
cluster.pig and recommender.pig com.sample.pig.PriceClusterUDF, com.sample.pig.PriceDiffUDF

The idea was to plot the msrp prices as histograms in R, view them and make decision on how to create clusters suggested by the visualization.

Please take a look at the Recommendation power point slides for details.
I created a Java UDF (com.sample.pig.PriceClusterUDF) to label a car based on it's MSRP Price value.
Then we loop thru the data (trim.csv) and attached appropriate price label.
We grouped (clustered) the given data by body type description (tc_body) and price label.
Please see cluster.pig for more details.
After clusters were composed. To return 5 similar cars to the given one was a process of determining to which group the given car belong and return 
the best five from the cluster. Please refer to recommender.pig for more details. 
The best five cars were computed based on how close in price they are from the given (observed car). I used com.sample.pig.PriceDiffUDF which
computed the difference between msrp price of the given car to the current msrp in that cluster. I sorted the differences in ascending order and returned the least deviated cars.    

# Notes/ideas use LSH on engine column and create cluster based on LSH #

  

