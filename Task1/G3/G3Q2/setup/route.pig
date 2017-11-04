--------------------------------------------------------------
-- NAME:
--     route.pig 
--
-- INPUT PARAMS
--     $PIG_IN_FILE
-- OUTPUT
--     $PIG_OUT_DIR
-- FIELDS
-- REFERENCE
--     http://www.transtats.bts.gov/Fields.asp?Table_ID=236
--------------------------------------------------------------
A = LOAD '$PIG_IN_DIR' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS 
(a1, a2, a3, a4, a5,
 a6:chararray, a7, a8:chararray, a9:chararray, a10,
 a11:chararray, a12:chararray, a13, a14, a15,
 a16, a17, a18:chararray, a19, a20,
 a21, a22, a23, a24:chararray, a25,
 a26, a27, a28:int, a29, a30,
 a31, a32, a33, a34, a35,
 a36, a37:int, a38, a39:int, a40,
 a41, a42:int, a43, a44:int, a45
);

-- a6   FlightDate
-- a9   Carrier
-- a11  FlightNumber
-- a12  Origin
-- a18  Dest
-- a24  CRSDepTime
-- a37  ArrDelay
-- a42  Cancelled
-- a44  Diverted
	
B = FILTER A by ( a28 is not null ) and ( a38 is not null ) and ( a42 == 0 ) and ( a44 == 0 ); 
C = FOREACH B GENERATE a6, a9, a11, a12, a18, a24, a37, a42, a44;  -- extract  

STORE C into '$PIG_OUT_DIR';  -- write the results to a folder

