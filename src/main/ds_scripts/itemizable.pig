
-- -Dpig.additional.jars=/Users/verak/Downloads/httpclient-4.4.jar:/Users/verak/Downloads/slf4j-api-1.7.12.jar:/Users/verak/Downloads/httpcore-4.4.jar:/Users/verak/Downloads/json-20141113.jar:

register '/Users/verak/Downloads/commons-io-2.4.jar'
register '/Users/verak/projects/demo/lib/jgenderize-master/target/jgenderize-1.2.jar';
register '/Users/verak/Downloads/jersey-client-2.13.jar';
REGISTER '/Users/verak/soft/elasticsearch-hadoop-2.0.2.jar';
REGISTER '/Users/verak/soft/elasticsearch-hadoop-pig-2.0.2.jar';


REGISTER '/Users/verak/projects/demo/target/pig-1.0-SNAPSHOT.jar';
define GenderGet com.sample.pig.GenderUDF;

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.resource=items1/days30');


A = LOAD 'items1/days30'
    USING org.elasticsearch.hadoop.pig.EsStorage();

AA = foreach A generate $0 as itemizable_id, $1 as line_items, $2 as variant_id, $3 as price, $4 as sku, $5 as upc, $6 as user_id, $7 as billing_first_name, $8 as billing_last_name,
   $9 as shipping_zip, $10 as shipping_state, $11 as shipping_city, $12 as total_price, $13 as description, 
   $14 as trial_order_id, $15 as vanity_started_at; 
A10 = limit AA 1000;
--AA10 = foreach A10 generate $0..$6, (chararray) $7, $8..$15; 
data = foreach A10 generate $0..$6, $7, GenderGet((chararray)$7) as sex, $8..$15;
--data = foreach A10 generate $0,$7;
--describe data;
store data into 'enchanced_items';
store data into 'items2/days30' USING EsStorage('es.http.timeout = 5m      es.index.auto.create = false'); 