-- this script is an exilirily script to compibe the data results from (pricePredictor.pig) and join it to the initail set
-- To Run the script
-- pig -x local -f aggregator.pig -param filename=answers-sample.csv

-- foreach details generate $record.$0, $0.$0, $0.$1, $0.$2, $0.$3, $1..$3, ($3-$2) as delta, $record.$10, (($3-$2) + $record.$8) as predicted;
-- sample data below for aggregation
-- 633434690	2011	63900	4	Automatic	63	20487.539682539682	22116.666666666668	1629.126984126986	24151	25084.126984126986		

 
data = load '$filename' using PigStorage('\t') as (transId:long, year:int, model_id:int, door: int, transmission:chararray, grp_count: int, avg_base_msrp:double, 
	 avg_trans_msrp:double, diff:double, msrp:int ,price_pred:double);

ans = foreach data generate $0, $10;
data1 = load 'hw2.csv' using PigStorage(',') as (sales_week:chararray, sales_date:chararray, transId:long, year:int, make:chararray, model_id:int, model:chararray, drive_type:chararray, door: int, transmission:chararray, base_msrp:int, 
	 engine:chararray, trans_msrp:int, destination: int, bodytype:chararray, zip:int, state:chararray, dealercash:int, custcash:int, f:int, l:int, cash:int, trim:chararray, trimId:long, makId:int, longitude:double, latitude:double);
total = join data1 by $2, ans by $0;
result = foreach total generate $0..$26, $28;

store result into 'predicted';