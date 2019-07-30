--First need to create directory in HDFS.Creating a directory called Airline_project

--bin/hadoop fs -mkdir /Airline_project

--Putting DataFiles into HDFS by Using below commands

--bin/hadoop fs -put /home/bhushan/Airline_Analysis_using_Apache_Pig/airports_mod.dat /Airline_project

--bin/hadoop fs -put /home/bhushan/Airline_Analysis_using_Apache_Pig/Final_airlines /Airline_project

--bin/hadoop fs -put /home/bhushan/Airline_Analysis_using_Apache_Pig/routes.dat /Airline_project


--open pig 

-- load all the datafiles from HDFS into relation using below commands

A = load '/Airline_project/airports_mod.dat' USING PigStorage(',') as {airport_id:int, airport_name:chararray, city:chararray, IATA_code:int, ICAO_code:int, lat:int, long:int, alt:int, time_zone_hours:int, dst:int}

B = load '/Airline_project/Final_airlines' USING PigStorage(',') as(airline_id:int, airline_name:chararray, alias: 
C = load '/Airline_project/routes.dat' USING PigStorage(',');

--from the dataset description we can identify the stuff for 
--airports_mod.dat == {$0-ID, $1-airport_name, $2-city, $3-country, $4-IATA_code, $5-ICAO_code, $6-lat, $7-long, $8-alt, $9-time_zone_hours, $10-DST(day_light_saving_time),$11-zone} 

--Final_airlines = {$0-ID, $1-name, $2-alias, $3-IATA, $4-ICAO, $5-call_sign, $6-country, $7-active/non_active_airlines}

--routes.dat = {$0-Airline_IATA/ICAO, $1-Airline_ID, $2-source_airport_IACO/IATA, $3-source_airport_ID, $4-destination_airport_IACO/IATA, $5-destination_airport_ID, $6-code_share, $7-number_of_stops, $8-code_for_plane_type}

                                                                  
                                                                 --#Problem_Statements#--

--A. Find list of Airports operating in the Country India 

b = filter A by $03 == 'India';
c = foreach b generate $01;
dump c;
store c into '/Airline_project/Airports_operating_in_India'; 

--B. Find the list of Airlines having zero stops 

b = join A by $0, C by $01;
c = filter C by $7 == 0;
d = foreach c generate $01;
dump d;
store d into '/Airline_project/Airlines_with_zero_stops';

--C. List of Airlines operating with code share

b = join B by $0, C by $01;
c = filter b by code_share == 'Y';
d = foreach c generate $01;
dump d;
store d into '/Airline_project/Airline_with_code_share';


--D. Which country (or) territory having highest Airports 

b = group A by country;
c = foreach b generate group, COUNT(A.airport_names);
d = order c by $01 DESC;
e = limit d 1;
dump e;


--E. Find the list of Active Airlines in United state 

b = join A by id, B by id;
c = filter b by $03 == 'United States';
d = filter c by active == 'Y';
e = dump d;
store d into '/Airline_project/Active_Airlines_in_US';


