REGISTER myudfs.jar;


INPUTS = LOAD './date.txt' USING PigStorage(',') AS (startDate:chararray,endDate:chararray);
INPUTS = FOREACH INPUTS GENERATE myudfs.DATENUM(startDate),myudfs.DATENUM(endDate);

DATAS = LOAD './0930.csv' USING PigStorage(',') AS (id:chararray,date:chararray,zone1,zone2,road,latitude:double,longitude:double);
DATAS = FOREACH DATAS GENERATE id,date,latitude,longitude;
DATAS = FILTER DATAS BY id !='編號';
DATAS = FOREACH DATAS GENERATE (int)id,myudfs.DATENUM(date),latitude,longitude;
DATAS = FOREACH DATAS GENERATE id,$1,latitude,longitude,INPUTS.$0,INPUTS.$1;
DATAS = FILTER DATAS BY ($1>=$4)AND($1<=$5);

DATAS = FOREACH DATAS GENERATE id,latitude,longitude;

TMP = FOREACH DATAS GENERATE id AS id2,$1 AS latitude2,$2 AS longitude2;
DATAS = CROSS DATAS,TMP;
DATAS = FOREACH DATAS GENERATE id,id2,myudfs.DISTANCE(latitude,longitude,latitude2,longitude2) AS distance;
DATAS = FILTER DATAS BY (id!=id2);
DATAS = FILTER DATAS BY (distance<(double)$distance);

ID_GROUP = GROUP DATAS BY id;
ID_COUNT = FOREACH ID_GROUP GENERATE $0,COUNT(DATAS);
ID_COUNT = ORDER ID_COUNT BY $1 DESC;

RESULT = FOREACH ID_COUNT GENERATE $0 AS id :int,$1 AS count:long;
RESULT = RANK RESULT BY count DESC;
RESULT = FOREACH RESULT GENERATE $0 as rank,id,count; 
RESULT = FILTER RESULT BY rank<=(int)$top_k;
RESULT = FOREACH RESULT GENERATE id,count ;


DUMP RESULT;
