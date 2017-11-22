class Flight(object):	
    
    def __init__(self, fields):
        
        self.fields = fields
        self.Year = int(fields[0])      #0 
        self.Month = int(fields[1])     #2
        self.Day = int(fields[2])       #3 
        self.Weekday = int(fields[3])   #4 
        self.FullDate = fields[4]       #5

        self.Airline = fields[5]        #7
        self.Carrier = fields[6]        #8 

        self.FlightNum = fields[7]      #10
        self.Origin = fields[8]         #11
        self.Dest = fields[9]           #17

        self.CRSDepTime = fields[10]     #23  
        str_depdelay = fields[11]        #25 
        if(str_depdelay is not None and len(str_depdelay) > 0):
            self.DepDelay = float(fields[11])
        else:
            self.DepDelay = 0.0

        self.CRSArrTime = fields[12]    #34
        str_arrdelay = fields[13]       #36
        if(str_arrdelay is not None and len(str_arrdelay) > 0):
            self.ArrDelay = float(fields[13])
        else:
            self.ArrDelay = 0.0
        
        #self.Cancelled = int(float(fields[12]))

    def __str__(self):
        return ','.join(self.fields)

#group_by_origin_airline = GROUP in BY (Origin, Carrier, AirlineID);

#average_ontime = FOREACH group_by_origin_airline 
#                 GENERATE FLATTEN(group) AS (Origin, Carrier, AirlineID), 
#                          AVG(in.DepDelay) AS performance_index;

#group_by_origin = GROUP average_ontime BY Origin; 
 
#top_ten_airlines = FOREACH group_by_origin {
#   sorted_airlines = ORDER average_ontime BY performance_index ASC;
#   top_airlines = LIMIT sorted_airlines 10;
#   GENERATE FLATTEN(top_airlines);
#}

#X = FOREACH top_ten_airlines GENERATE TOTUPLE( TOTUPLE( 'origin',$0), TOTUPLE( 'carrier',$1), TOTUPLE('airline', $2 )), TOTUPLE($3);
