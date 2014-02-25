-- loads the data and predicts prices by computing delta of trans_msrp and base_msrp divides the difference by 2.
-- and adds it to current base_msrp
-- removed the headers, and engine and trim chararray from teh set
------- this macro filters out irrelavent groups 
------- current record is mapped to it's cluster 
------- and price is predicted, prediction is based on base_msrp and delta
--------delata is computed as difference of average values of transaction_msrp and base_msrp of that group  
DEFINE predictPrice(grp, record)
RETURNS delta
{	
	filtered = filter $grp by ($record.$1 == $0.$0 AND $record.$3 == $0.$1 AND $record.$6 == $0.$2 AND $record.$7 == $0.$3);
	details = foreach filtered generate group, COUNT($1.$0),AVG($1.$10) as avgMsrp, AVG($1.$11) as avgTrnMsrp;
	d = foreach details generate $record.$0, $0.$0, $0.$1, $0.$2, $0.$3, $1..$3, ($3-$2) as delta;
	dd = foreach d generate $0..$8, $record.$8;
	$delta = foreach dd generate $0..$9, (double)(0.5 * $8) + (double)($9);		
}

-------
------- this macro retrieves a record from a set based on the transaction_id
-------
DEFINE getComponents(data,transId)
RETURNS details
{
	details1 = filter $data by ($2==$transId);
	$details = foreach details1 generate $2..$11;				
}

-------
------- this macro loops thru hw2 set 
-------
DEFINE compute(transId, data, grp)
RETURNS r
{
	row = getComponents($data, $transId);	
	$r = predictPrice($grp, row);					
}


A = load 'hw2-noeng.csv' using PigStorage('\t') as (sales_week:chararray, sales_date:chararray, transId:long, year:int, make:chararray, model_id:int, model:chararray, drive_type:chararray, door: int, transmission:chararray, base_msrp:int, 
	 trans_msrp:int, destination: int, bodytype:chararray, zip:int, state:chararray, dealercash:int, custcash:int, f:int, l:int, cash:int, trimId:int, makId:int, longitude:double, latitude:double);
/**
AA = distinct A;

result1 = foreach AA generate $0..$24;
result2 = foreach result1 generate $0..$24, (double)($10-$11);
result3 = foreach result2 generate $0..$24, ABS($25);
result4 = foreach result3 generate $0..$25, $25+$10;
result5 = foreach result4 generate $0..$24, $26;
store result5 into 'predicted';
**/

dataD = distinct A;
grp = group dataD by ($3, $5, $8, $9);
-- $3 - year, $5-model_id, $8-door, $9-transmission
-- 8452685
--8464766
B = compute($transId, dataD, grp);
store B into 'A-$transId';
--B = compute(8464766, dataD, grp);
--store B into 'A-8464766';
