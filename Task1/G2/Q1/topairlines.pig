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
	
group_by_origin_airline = GROUP in BY (Origin, Carrier, AirlineID);

average_ontime = FOREACH group_by_origin_airline 
                 GENERATE FLATTEN(group) AS (Origin, Carrier, AirlineID), 
                          AVG(in.DepDelay) AS performance_index;

group_by_origin = GROUP average_ontime BY Origin; 
 
top_ten_airlines = FOREACH group_by_origin {
   sorted_airlines = ORDER average_ontime BY performance_index ASC;
   top_airlines = LIMIT sorted_airlines 10;
   GENERATE FLATTEN(top_airlines);
}

--X = FOREACH top_ten_airlines GENERATE TOTUPLE( TOTUPLE( 'origin',$0), TOTUPLE( 'carrier',$1), TOTUPLE('airline', $2 )), TOTUPLE($3);

X = FOREACH top_ten_airlines GENERATE  $0, $1, $2, $3;


--STORE X into 'cql://temp/t1g2q1?output_query=update%20t1g2q1%20set%20ontimeratio%20%3D%3F' USING CqlStorage();  

STORE X into '$PIG_OUT_DIR';  -- write the results to a folder

-----------------------------
-----------------------------
--pig -x local -param PIG_IN_DIR=/data/G2 -param PIG_OUT_DIR=/data/G2Q1 -f ./topairlines.pig >& result4.txt
