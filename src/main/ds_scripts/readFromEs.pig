REGISTER '/Users/verak/soft/elasticsearch-hadoop-2.0.2.jar';
REGISTER '/Users/verak/soft/elasticsearch-hadoop-pig-2.0.2.jar';


REGISTER '/Users/verak/projects/demo/target/pig-1.0-SNAPSHOT.jar';
define gender com.sample.pig.GenderIdentifierUDF;

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.resource=ret/year12');


A = LOAD 'ret/year12'
    USING org.elasticsearch.hadoop.pig.EsStorage();
    
B = forearch A generate A._source.campaign;

A10 = limit B 10;
DUMP A10;

describe A10;
/**
B  = foreach A generate $0,A.channel as channel,  A.campaign as campaign, A.campaign_cost as campaign_cost, A.nbr_trial_users as trial_users, A.nbr_subscription_users as nbr_subs_users, A.nbr_buyers as nbr_buyers, A.nbr_trial_to_sub_conversion_rate as conv_rate2, A.registration_date as regist_date;
dump B;

**/
--dump data;
--store data into 'users/orders' USING EsStorage('es.http.timeout = 5m      es.index.auto.create = false'); 