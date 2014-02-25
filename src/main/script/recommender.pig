-- to run this script you need to pass $trimId
-- sample to execute from command line
-- pig -x local -f recommender.pig -param trimId=330541

REGISTER '../../../target/pig-1.0-SNAPSHOT.jar';

DEFINE sensetivity com.sample.pig.PriceDiffUDF;


DEFINE recomsFullCluster(grp)
RETURNS cars
{
	data = load 'cl-price-body-type.csv' using PigStorage('\t') as (year:int, make:chararray, model:chararray, trim:chararray, tr_id: int, tc_body:chararray, msrp:int, inv:int, pct_dist:double, 
	ind_trans: int, cl: tuple(prcLable:int, tcb:chararray), clSize:int, trmId:int);
	data1 = foreach data generate $0..$11;
	$cars = filter data1 by ($10.$0 == $grp.$0 AND $10.$1 == $grp.$1);	
}

DEFINE getGrpByTrimId(trimId)
RETURNS grp
{
	data = load 'cl-price-body-type.csv' using PigStorage('\t') as (year:int, make:chararray, model:chararray, trim:chararray, tr_id: int, tc_body:chararray, msrp:int, inv:int, pct_dist:double, 
	ind_trans: int, cl: tuple(prcLable:int, tcb:chararray), clSize:int, trmId:int);
	data1 = foreach data generate $0..$11;
	filtered = filter data1 by ($4 == $trimId);
	$grp = foreach filtered generate $10, $6;	
}


grpName = getGrpByTrimId($trimId);
grpName = foreach grpName generate flatten($0), $1;

-- tuple(17000,Sedan)
similars = recomsFullCluster(grpName);
--store similars into 'sameCluster';
--dump similars;
-- only 5 cars 
pricemain = foreach grpName generate $2;
simDetails = foreach similars generate $0..$11, sensetivity(pricemain.$0, $6);
simOrdered = order simDetails by $12 asc;
-- $4 - trim id
simOrdIds = foreach simOrdered generate $4;
--cars5 = limit simOrdered 5;
cars5 = limit simOrdIds 5;

--dump cars5;
store cars5 into 'cars5-$trimId';