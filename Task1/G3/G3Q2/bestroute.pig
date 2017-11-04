----------------------------------------------------------
-- bestroute.pig
-- INPUT
--     $PIG_INPUT_FILE
-- OUTPUT
--     temp:1g3q2
----------------------------------------------------------

-- a6   FlightDate
-- a9   Carrier
-- a11  FlightNumber
-- a12  Origin
-- a18  Dest
-- a24  CRSDepTime
-- a37  ArrDelay
-- a42  Cancelled
-- a44  Diverted

in  = LOAD '$PIG_IN_DIR' AS ( FlightDate:chararray, Carrier:chararray, FlightNumber:chararray,  Origin:chararray, Dest:chararray, CRSDepartureTime:chararray, ArrDel:int, Cancelled:int, Diverted:int);
 
A = FILTER in BY (CRSDepartureTime < '1200' );
B = FILTER in BY (CRSDepartureTime > '1200' );

A1 = FOREACH A GENERATE ToDate(FlightDate, 'yyyy-MM-dd') AS FlightDate, Carrier, FlightNumber, Origin, Dest, CRSDepartureTime, ArrDel;
B1 = FOREACH B GENERATE ToDate(FlightDate, 'yyyy-MM-dd') AS FlightDate, Carrier, FlightNumber, Origin, Dest, CRSDepartureTime, ArrDel;

a_group_by_date_orig_dest = GROUP A1 BY ( FlightDate, Origin, Dest );

a_best_flight = FOREACH a_group_by_date_orig_dest {
                    ord = ORDER A1 BY ArrDel ASC;
                    top = LIMIT ord 1;
                    GENERATE FLATTEN(top);  }  


b_group_by_date_orig_dest = GROUP B1 BY ( FlightDate, Origin, Dest );

b_best_flight = FOREACH b_group_by_date_orig_dest {
                    ord2 = ORDER B1 BY ArrDel ASC;
                    top2 = LIMIT ord2 1;                 
				    GENERATE FLATTEN(top2); 		  
					}					


join_ab = JOIN a_best_flight BY (Dest, FlightDate), b_best_flight BY (Origin, SubtractDuration(FlightDate,'P2D') );

-- X = FOREACH join_ab GENERATE TOTUPLE( TOTUPLE('flight_date', ToString($0,'dd/MM/yyyy') ),TOTUPLE('first_leg_origin', $3 ),TOTUPLE('first_leg_dest', $4 ),TOTUPLE('second_leg_dest', $11 )),TOTUPLE($1, $5, $2, $8, $12, ToString($7,'dd/MM/yyyy'), $9 ); 

X = FOREACH join_ab GENERATE $0, $3, $4, $11, $1, $5, $2, $8, $12,  $9; 

--STORE X into 'cql://temp/t1g3q2?output_query=update t1g3q2 set first_leg_carrier%3D%3F,first_leg_departure_time%3D%3F,first_leg_flight_number%3D%3F,second_leg_carrier%3D%3F,second_leg_departure_time%3D%3F,second_leg_flight_date%3D%3F,second_leg_flight_number%3D%3F' USING CqlStorage(); 

STORE X into '$PIG_OUT_DIR';

---pig -x local -param PIG_IN_DIR=/data/G3Q2 -param PIG_OUT_DIR=/data/RG3Q2 -f ./bestroute.pig 

