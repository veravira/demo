A = load 'hw2-noeng.csv' using PigStorage('\t') as (sales_week:chararray, sales_date:chararray, transId:long, year:int, make:chararray, model_id:int, model:chararray, drive_type:chararray, door: int, transmission:chararray, base_msrp:int, 
	 trans_msrp:int, destination: int, bodytype:chararray, zip:int, state:chararray, dealercash:int, custcash:int, f:int, l:int, cash:int, trimId:int, makId:int, longitude:double, latitude:double);
	 
tds1 = foreach A generate $2;
tds = distinct tds1;
store tds into 'unique-tds';	 