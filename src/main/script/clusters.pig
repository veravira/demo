-- file location is pig/src/main/script/clusper.pig
-- target folder is under same lavel as src
REGISTER '../../../target/pig-1.0-SNAPSHOT.jar';

DEFINE cluster com.sample.pig.PriceClusterUDF;

data = load 'trims.csv' using PigStorage(',') as (year:int, make:chararray, model:chararray, trim:chararray, tr_id: int, tc_body:chararray, msrp:int, inv:int, pct_dist:double, ind_trans: int);
filtered = filter data by ($6 is not null);
data1 = foreach filtered generate $0..$5, $6, cluster($6);
--store data1 into 'data-with-label';

data2 = group data1 by ($7, $5);

-- store data2 into 'data2'; -- to count how many groups
-- we have 123 clusters 

d1 = foreach data2 generate group, COUNT($1.$0),flatten($1.$4);
 
dd1 = order d1 by $1 desc;
dd2 = join data by $4, dd1 by $2;

store dd2 into 'grp-joined';
