FYI: Docker could not be installed, as my laptop kept showing error, which I could not resolve in the present time.

Details: Two part of code, 1)multitest.py: Reads the csv file and ingest it 
			   2) Makes the aggregate table
Pre-Req:1) python 3.5+
	2)postgres ver 9+
	3)Libraries: Pandas, os,sys,time,psycopg2,contextlib,csv(version is not an issue)


a)
Steps to Run:
1)Install postgres edit host, dbname,user, password,port values in multi_test.py and aggregate.py
2) make table in postgres 'test_data' with column name: name,sku(primary key),description. 
3) make another table 'agg' with column name: name, no of products
4) any change in table mentioned in '2,3' should be copies in both the .py files
5) Run the multi_test.py

b)
Tables:
1) test_data: Create this table on postgres terminal by right clocking or in the query table	, keep the data type as 'varchar' or 'text' either will do. 'sku' is the primary key, so keep this constraint in check. 
Columns:['name','sku','description']
CREATE TABLE public.test_data
(
    name character varying,
    sku character varying,
    description character varying
    PRIMARY KEY (sku)
)
WITH (
    OIDS = FALSE
);

ALTER TABLE public.test_data
    OWNER to postgres;

2)agg: The aggregate table, create on postgres UI or by create command on its sql terminal.Both can be char or 'no of products' can be int based on future use. I kept it as varchar/text. 
Columns:['name','no of products']
CREATE TABLE public.agg
(
    name character varying,
    no of products character varying,
    
)
WITH (
    OIDS = FALSE
);

ALTER TABLE public.agg
    OWNER to postgres;


c,e)For the point to achieve, upsert is used in the sql query to prevent the duplicacy of record and to prevent from code to crash in case of duplicate record is found. It updates it with new found value
10 rows data set was not time challenging so I practiced with 50k data. It's more or less the same experience for me.
All the points were tried to achieve, in case of more time if I would have been given I would have compared multi threading and multiprocessing instead of pool connection which is also fast. 
The csv file is such that some cells have multiple lines, thus making it difficult to transfer data, throwing errors in most of the cases. Line to line read is slow, which is being applied. 
I want to look into that issue quite closely, as to why this is happening.
A possible try was indexing, which would have certain trade-off , but I would like to try it too