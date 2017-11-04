-------------------------------------
-- airport.pig 
-- INPUT 
--     $PIG_INPUT_FILE
-- OUTPUT
--     stout
-- FIELDS
--     a1  - passengers
--     a14 - origin airport 
--     a21 - destination airport
-------------------------------------
A = LOAD '$PIG_INPUT_FILE' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS 
(a1:int, a2:int, a3:int, a4:int, a5:chararray,
 a6:int, a7:chararray, a8:chararray, a9:chararray, a10:chararray,
 a11:chararray, a12:int, a13:int, a14:chararray, a15:chararray,
 a16:int, a17:chararray, a18:chararray, a19:chararray, a20:int,
 a21:chararray, a22:chararray, a23:int, a24:chararray, a25:chararray);
	
B = foreach A generate a1, a14, a21;    

DUMP B;
