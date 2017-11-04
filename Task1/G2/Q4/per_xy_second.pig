--------------------------------------------------------------
-- NAME:
--     topairlines.pig 
--
-- INPUT PARAMS
--     $PIG_IN_DIR
-- OUTPUT
--     $PIG_OUT_DIR
-- FIELDS
--     airlineID 
--     Origin
--     Dest
--     DepDel15
--     ArrDel15
-- REFERENCE
--     http://www.transtats.bts.gov/Fields.asp?Table_ID=236
--------------------------------------------------------------

in  = LOAD '$PIG_IN_DIR' AS 
          ( AirlineID:chararray, --a8
            Carrier:chararray,   --a9
            FlightNum:int,       --a11
            Origin:chararray,    --a12       
            Dest:chararray,      --a18
            DepDelay:float,      --a26
            DepDel15:float,      --a28
            ArrDelay:float,      --a37 
            ArrDel15:float       --a39
          );
	
group_by_origin_dest = GROUP in BY (Origin, Dest);

average_ontime = FOREACH group_by_origin_dest
               GENERATE FLATTEN(group) AS (Origin, Dest),
               AVG(in.ArrDelay) AS performance_index;

--X = FOREACH average_ontime GENERATE TOTUPLE( TOTUPLE('origin',$0), TOTUPLE('dest', $1),TOTUPLE('avgDelay', $2));


--STORE X into 'cql://temp/t1g2q1?output_query=update%20t1g2q1%20set%20ontimeratio%20%3D%3F' USING CqlStorage();  

STORE average_ontime into '$PIG_OUT_DIR';  -- write the results to a folder

-----------------------------
-----------------------------

 


--pig -x local -param PIG_IN_DIR=/data/G2 -param PIG_OUT_DIR=/data/G2Q1 -f ./topairlines.pig >& result4.txt
