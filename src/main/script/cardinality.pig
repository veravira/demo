-- load data from sku_to_description.csv
-- constructs cardinality space per size
-- -param size=2
-- pig -Dpig.additional.jars=/Users/verak/Downloads/httpclient-4.4.jar:/Users/verak/Downloads/slf4j-api-1.7.12.jar:/Users/verak/Downloads/slf4j-log4j12-1.7.7.jar  -param size=2 -x local cardinalityBuilder.pig 
-- slf log4j --  /Users/verak/Downloads/slf4j-log4j12-1.7.7.jar 

REGISTER '/Users/verak/projects/demo/target/pig-1.0-SNAPSHOT.jar';
define vector com.sample.pig.BundleToVecUDF;


-- 817810017759,Diapers, Sleeping Bears, Size N
data = load '/Users/verak/projects/sql_data/sku_to_description.csv' using PigStorage(',') as (sku:chararray, dstr:chararray, dname:chararray, dsize:chararray);    

-- to see all distinct prints

-- d1 = foreach data generate $2;
-- d2 = distinct d1;
-- d3 = order d2 by $0 ASC;
-- dump d3;

-- Blue Gingham	64	1
-- Anchors & Stripes,Blue Gingham	28	2
b = load '/Users/verak/projects/pigs/all/size$size' using PigStorage('\t') as (bundleVal:chararray, seen:int, bsize:int);    

b_6 = filter b by bsize == 6;

bb_6 = foreach b_6 generate (tuple (chararray,chararray, chararray, chararray, chararray, chararray))STRSPLIT(bundleVal,',',6) as t, seen, bsize;
b_6Vec = foreach bb_6 generate vector(t), seen, bsize;

test = limit b_6Vec 10;
dump test;
