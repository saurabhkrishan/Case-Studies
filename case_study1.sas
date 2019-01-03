libname vdir "/folders/myfolders";


OPTIONS COMPRESS=YES FIRSTOBS=1 OBS=100000;

/*Fetching The Flight data File*/
PROC IMPORT DATAFILE='/folders/myfolders/SAS Master Case study 1 - Updated files (1)/SAS Case study 1 files/flights.csv'
		    OUT=WORK.FLIGHTS
		    DBMS=CSV
		    REPLACE;
GETNAMES=YES;
RUN;

/*QUESTION 1*/

/*1.1*/
DATA WORK.flightsNEW(DROP=sched_dep_time dep_time sched_arr_time arr_time);
SET WORK.flights;
Y=YEAR(DATE);
M=MONTH(DATE);
D=DAY(DATE);
SCH_DEP_TIME = input(put(sched_dep_time,z4.),hhmmss4.);
format SCH_DEP_TIME time5. ;
DEPA_TIME = input(put(dep_time,z4.),hhmmss4.);
format DEPA_TIME time5. ;
SCH_ARR_TIME = input(put(sched_arr_time,z4.),hhmmss4.);
format SCH_ARR_TIME time5.;
ARRI_TIME= input(put(arr_time,z4.),hhmmss4.);
format ARRI_TIME time5.;
DEPA_DIFF=INTCK('MINUTE',SCH_DEP_TIME,DEPA_TIME);
IF DEPA_DIFF<30 THEN DEP_STATUS="EARLY DEPARTURE";
ELSE DEP_STATUS="DELAY DEPARTURE";
ARR_DELAY=INTCK('MINUTE',SCH_ARR_TIME,ARRI_TIME);
IF ARR_DELAY<30 THEN ARR_STAT="EARLY STATUS";
ELSE ARR_STAT="DELAY ARRIVAL";
RUN;
/*QUESTION 2*/

/*importing weather dataset*/
PROC IMPORT DATAFILE='/folders/myfolders/SAS Master Case study 1 - Updated files (1)/SAS Case study 1 files/weather1.csv'
	DBMS=CSV
	OUT=WORK.WEATHER;
	GETNAMES=YES;
RUN;

/*creating H variables for Hour*/
DATA WORK.WEATHERNEW;
SET WORK.WEATHER;
newTIME=INPUT(time,HHMMSS4.);
H=HOUR(newTIME);
if h=0 then h=24;
RUN;
DATA WORK.FLIGHTSNEW;
SET work.flightsnew;
H=HOUR(SCH_DEP_TIME);
if h=0 then h=24;
RUN;
data vdir.flightsNEW;
set work.flightsNEW;
run;
/*Sorting Datasets before merging*/
PROC SORT DATA=WORK.flightsNEW;
BY H DATE;
RUN;
PROC SORT DATA=WORK.WEATHERNEW;
BY H DATE;
RUN;
/*merging flights and weather datasets*/
DATA WORK.FANDW;
MERGE WORK.FLIGHTSNEW(IN=A) WORK.WEATHERNEW(IN=B);
BY H;
IF A;
RUN;
/*importing planes datasets*/
PROC IMPORT DATAFILE='/folders/myfolders/SAS Master Case study 1 - Updated files (1)/SAS Case study 1 files/planes.csv'
	DBMS=CSV
	OUT=WORK.PLANES;
	GETNAMES=YES;
RUN;
DATA WORK.PLANESNEW;
SET WORK.PLANES;
TAILNUM=PLANE;
RUN;

/*merging flights and planes datasets*/
DATA WORK.FANDP;
MERGE vdir.flightsNEW(IN=A) WORK.PLANESNEW(IN=B);
BY TAILNUM;
IF A AND B;
RUN;
/*
PROC SQL;
CREATE TABLE WORK.FANDPSQL AS
SELECT * FROM WORK.flightsNEW A 
INNER JOIN WORK.PLANES B
ON A.TAILNUM=B.PLANE;
QUIT;
*/
DATA WORK.FANDP;
SET WORK.FANDP;
YEAR_USE=YEAR(DATE)-manufacturing_year;
RUN;

/*QUESTION 3*/
/*PROC MEANS DATA=WORK.FLIGHTS NMISS;
RUN;*/

/*creating format for missing and not missing obs*/
proc format; 
value $missfmt ' '='Missing' other='Not Missing';
 value  missfmt  . ='Missing' other='Not Missing';
run;

/*calculating the missing values for all the variables*/
proc freq data=work.flights;
format _char_ $missfmt.;
table _char_ / missing missprint nocum nopercent;
format _NUMERIC_ missfmt.;
tables _NUMERIC_ / missing missprint nocum nopercent;
run;

/*deleting missing values*/
DATA WORK.FLIGHTALTERED;
SET WORK.flights;
IF TAILNUM=" " THEN DELETE;
ELSE IF DEP_TIME=. OR ARR_TIME=. THEN DELETE;
RUN;

/*imputating missing values with mean*/
PROC STDIZE data=WORK.FLIGHTSNEW OUT=WORK.FLIGHTSIMPUTED
REPONLY
METHOD=MEAN;
VAR ARR_DELAY;
RUN;

/*other way by finding means of variable*/

proc means data=work.flightsnew mean;
var arr_delay air_time;
output out=work.flightmeans;
run;

/*transfering means values to the macros*/

data work.flightmeans;
set work.flightmeans;
if _stat_="MEAN" THEN
CALL SYMPUT('ARR_DELAY_M',ARR_DELAY);
IF _stat_="MEAN" THEN 
CALL SYMPUT('AIR_TIME_M',AIR_TIME);
RUN;

/*printing macros*/
%PUT &ARR_DELAY_M &AIR_TIME_M;

/*replacing missing values with macro values*/
data work.flightsdataimputed;
set work.flightsnew;
if arr_delay=. then arr_delay=&ARR_DELAY_M;
if AIR_TIME=. then AIR_TIME=&AIR_TIME_M;
run;

/* for debugging purpose
proc means data=work.flightsdataimputed mean;
run;*/


PROC IMPORT DATAFILE='/folders/myfolders/SAS Master Case study 1 - Updated files (1)/SAS Case study 1 files/weather1.csv'
	DBMS=CSV
	OUT=WORK.weather;
	GETNAMES=YES;
RUN;

proc freq data=work.weather;
format _char_ $missfmt.;
table _char_ / missing missprint nocum nopercent;
format _NUMERIC_ missfmt.;
tables _NUMERIC_ / missing missprint nocum nopercent;
run;

/*Imputing missing observations of weather dataset with mean of variable*/ 

proc stdize data=work.weather out=work.weatherdataimputed
reponly
method=mean;
var temp	dewp	humid	wind_dir	wind_speed	wind_gust	precip	pressure	visib;
run;

/*Importing Planes Datasets*/ 
PROC IMPORT DATAFILE='/folders/myfolders/SAS Master Case study 1 - Updated files (1)/SAS Case study 1 files/planes.csv'
	DBMS=CSV
	OUT=WORK.planes;
	GETNAMES=YES;
RUN;

/*Finding missing values in planes data set*/
proc freq data=work.planes;
format _char_ $missfmt.;
table plane / missing missprint nocum nopercent out=work.planes_plane;
table type / missing missprint nocum nopercent out=work.planes_types;
table model / missing missprint nocum nopercent out=work.planes_model;
table manufacturer / missing missprint nocum nopercent out=work.planes_manufacturer;
table speed / missing missprint nocum nopercent out=work.planes_speed;
format _numeric_ missfmt.;
tables engines / missing nocum nopercent out=work.planes_engines;
tables seats / missing nocum nopercent out=work.planes_seats;
tables fuel_cc / missing nocum nopercent out=work.planes_fuel_cc;
run;

/*Speed Variable has 90 % missing value*/
data work.planesnew;
set work.planes(drop=speed);
run;

/*Deleting all missing observations*/
data work.planesdeleted;
set work.planesnew;
if cmiss(of _char_)  then delete;
if nmiss(of _numeric_) then delete;
run;

/*Question 4*/
/*Renaming Variables as per data dictionery*/
/*4.1*/
data work.flightsformated;
set work.flights(rename=(date=date_of_departure dep_time=Actual_departure arr_time=actual_arrival_times
sched_dep_time=Scheduled_departure sched_arr_time=scheduled_arrival carrier=carrier_code flight=Flight_number
tailnum=Plane_tail_number origin=Origin dest=destination));
run;

/* Providing Labels to Variables */
data work.flightsformated_labels;
set work.flightsformated;
label
date_of_departure	=	"	date of departure	"	
Actual_departure= "	Actual departure times	"	
actual_arrival_times	=	"	Actual arrival times	"	
Scheduled_departure =" Scheduled departure times"
scheduled_arrival	=	"	Scheduled arrival times	"	
carrier_code	=	"	Two letter carrier abbreviation	"	
Flight_number	=	"	Flight number	"	
Plane_tail_number	=	"	Plane tail number	"	
Origin="Origin "
destination	=	"	destination"	
distance	=	"	Distance flown	"	
air_time	=	"	Amount of time spent in the air, in minutes	";
run;

/*Renaming Weather Data set*/
DATA WORK.WEATHERrenamed;
set work.weather(rename=(
origin	=	Origin
date	=	date_of_recording
time	=	Time_of_recording
temp	=	Temperature_in_F
dewp	=	dewpoint_in_F
humid	=	Relative_humidity
wind_dir	=	Wind_direction_in_degrees
wind_speed	=	Wind_speed
wind_gust	=	Wind_gust_speed_in_mph
precip	=	Preciptation_in_inches
pressure	=	Sea_level_pressure_in_millibars
visib	=	Visibility_in_miles
));
run;

/*Labelling Weather data set*/

DATA WORK.WEATHElabelled;
set work.weather;
label
origin	=	"Origin"	
date	=	"date of recording"
time	=	"Time of recording"	
temp	=	"Temperature in F"	
dewp	=	"dewpoint in F"	
humid	=	"Relative humidity"	
wind_dir	=	"Wind direction (in degrees)"
wind_speed	=	"Wind speed"	
wind_gust	=	"Wind gust speed (in mph)"	
precip	=	"Preciptation, in inches	"
pressure	=	"Sea level pressure in millibars"
visib	=	"Visibility in miles"	;
run;

/*Planes Datasets*/

data work.planesrenamed;
set work.planes(rename=(
plane	=	Tail_number
manufacturing_year	=	Year_manufactured
type	=	Type_of_plane
manufacturer=Manufacturer
model	=	 Model
engines	=	Number_of_engines
seats	=	Number_of_seats
speed	=	Average_cruising_speed_in_mph
engine	=	Type_of_engine
fuel_cc	=	Avg_yearly_fuel_consumption_cost
));

data work.planeslabelled;
set work.planes;
label
plane	=	"Tail number"
manufacturing_year	=	"Year manufactured"
type	=	"Type of plane"
manufacturer="Manufacturer"
model	=	 "model"
engines	=	"Number of engines"
seats	=	"Number of seats"
speed	=	"Average cruising speed in mph"
engine	=	"Type of engine"
fuel_cc	=	"Average annual fuel consumption cost";
run;

/*Airports*/
PROC IMPORT DATAFILE='/folders/myfolders/SAS Master Case study 1 - Updated files (1)/SAS Case study 1 files/airports.csv'
	DBMS=CSV
	OUT=WORK.airports;
	GETNAMES=YES;
RUN;
/*renaming*/

data work.airportrenamed;
set work.airports(rename=(
faa	=	FAA_airport_code
name	= Name_of_Aiport
lat= Latitude
lon	=	Longitude
));

/*labelling*/
data work.airportabelled;
set work.airports;
label
faa	=	"FAA airport code"
name	= "Name of Aiport"
lat= "Latitude"
lon	=	"Longitude";
run;

/*Airlines*/
PROC IMPORT DATAFILE='/folders/myfolders/SAS Master Case study 1 - Updated files (1)/SAS Case study 1 files/airlines.csv'
	DBMS=CSV
	OUT=WORK.airlines;
	GETNAMES=YES;
RUN;
/*labelling airlines data set*/
data work.airlineslabelled;
set work.airlines;
label
Fight_carrier_Code	= "Two letter abbreviation"
name	= "Full name";
run;

/*Question 4.2*/
/*rounding the fuel_cc variable and giving format*/
DATA WORK.PLANES2;
SET WORK.PLANES;
FUEL_CC=ROUND(FUEL_CC,.01);
FORMAT FUEL_CC DOLLAR10.2;
RUN;
/*QUESTION 4.3*/
/*changing flight code from numeric to character*/
DATA WORK.FLIGHTSUNIQUE(DROP=FLIGHT);
SET WORK.FLIGHTS(RENAME=(carrier=Fight_carrier_Code));
FLIGHTCODE=PUT(FLIGHT, $15.);
RUN;
/*sorting flight data by carrier code*/
PROC SORT DATA=WORK.FLIGHTSUNIQUE OUT=WORK.flightsuniqueSORTED;
BY Fight_carrier_Code;
RUN;
/*merging flight and airlines dataset*/
DATA WORK.FLIGHT_AIRLINES(RENAME=(NAME=AIRLINES_NAME));
MERGE WORK.flightsuniqueSORTED(IN=A) WORK.AIRLINES(IN=B);
BY Fight_carrier_Code;
IF A;
RUN;
/*sorting and renaming flights data for merging to get origin airports*/
PROC SORT DATA=WORK.flightsuniqueSORTED OUT=WORK.flightsuniqueSORTED(RENAME=ORIGIN=FAA_airport_code);
BY ORIGIN;
RUN;
PROC SORT DATA=WORK.AIRPORTRENAMED;
BY FAA_airport_code;
RUN;
/*merging flight airport data*/
DATA WORK.FLIGHT_AIRPORT;
MERGE WORK.flightsuniqueSORTED(IN=A) WORK.AIRPORTRENAMED(IN=B);
BY FAA_airport_code;
IF A;
RUN;

DATA WORK.AIRPORT_LABELS;
SET WORK.AIRPORTRENAMED;
RENAME FAA_airport_code=DEST ;
RENAME NAME_OF_AIPORT=DEST_AIRPORT;
RUN;
/*CONCATINATING ORIGIN, DESTINATION'S LATITUDE AND LONGITUDES*/
DATA WORK.FLIGHT_AIRPORT;
SET WORK.FLIGHT_AIRPORT(RENAME=(FAA_airport_code=ORIGIN_PORT NAME_OF_AIPORT=ARRIVAL_PORT));
ARR_LATT_LONG=CATX(", ",LATITUDE,LONGITUDE);
RUN;
DATA WORK.FLIGHT_AIRPORT;
SET WORK.FLIGHT_AIRPORT(DROP=LATITUDE LONGITUDE);
RUN;
/*Sorting Dataset by destination*/
PROC SORT DATA=WORK.FLIGHT_AIRPORT;
BY dest;
RUN;
DATA WORK.FLIGHT_AIRPORT;
MERGE WORK.FLIGHT_AIRPORT(IN=A) WORK.AIRPORT_LABELS(IN=B);
BY DEST;
IF A;
RUN;

DATA WORK.FLIGHT_AIRPORT(DROP=LATITUDE LONGITUDE);
SET WORK.FLIGHT_AIRPORT;
DEST_LAT_LONG=CATX(", ",LATITUDE,LONGITUDE);
rename dest=DEST_PORT;
RUN;
/*QUESTION 5*/
/*Busiest Route*/
DATA WORK.FLIGHTROUTE(DROP=ORIGIN_PORT DEST_PORT);
SET WORK.flight_airport;
ROUTE=CATX("-",ORIGIN_PORT,DEST_PORT);
RUN;
/*transferring file to ouptut directory*/
data vdir.FLIGHTROUTE;
set work.flightroute;
run;
PROC FREQ data=vdir.FLIGHTROUTE;
TABLE ROUTE / NOCOL NOCUM NOPERCENT OUT=WORK.BUSIESTROUTE;
RUN;
PROC SORT DATA=WORK.BUSIESTROUTE;
BY descending COUNT;
RUN;
/*JFK-LAX is the MOST FLOWN ROUTE*/

/*Top 5 routes with carriers*/
/*merging flight route data with airlines dataset*/
proc sort data=vdir.FLIGHTROUTE out=FLIGHTROUTEsorted;
by Fight_carrier_Code;
run;
/*Top 5 busiest route*/
data work.top5routes;
set work.busiestroute(obs=5);
run;
/*merging flights */
data work.FLIGHTROUTE_withairlines;
merge work.FLIGHTROUTEsorted work.airlines;
by Fight_carrier_Code;
run;
data work.FLIGHTROUTE_withairlinestop;
set work.flightroute_withairlines;
where route="JFK-LAX" or   route="LGA-ATL" or route="LGA-ORD" or route="JFK-SFO" or route="LGA-CLT";
run;
/*frequency of flights with airlines on top 5 routes*/
proc freq data=work.flightroute_withairlinestop;
table name / nocol nocum nopercent out=airlinesontop5route;
run;
/*comparison from top 5 routes with all*/
data work.airlinesontop5route(rename=(name=Airline count=top5_frequency));
set work.airlinesontop5route(keep=name count);
run;
proc freq data=work.flightroute_withairlines;
table name/ nocol nocum nopercent out=work.no_of_flightsbycarriers;
run;
proc sort data=work.no_of_flightsbycarriers out=work.no_of_flightsbycarriers(rename=(name=Airline count=tota_flights));
by name;
run;
proc sort data=work.airlinesontop5route;
by airline;
run;
data work.comparison(keep=airline tota_flights top5_frequency);
merge work.no_of_flightsbycarriers(in=a) work.airlinesontop5route(in=b);
by airline;
if a;
run;

/*Question 6*/
/*BUSIEST TIME OF THE DAY*/
DATA WORK.FLIGHTBUSIEST(DROP=sched_dep_time);
SET WORK.FLIGHT_AIRLINES;
SCH_DEP_TIME = input(put(sched_dep_time,z4.),hhmmss4.);
format SCH_DEP_TIME time5. ;
HOUROFTIME=HOUR(SCH_DEP_TIME);
FLIGHTDATE=DATE;
HOURS=HOUROFTIME;
FORMAT FLIGHTDATE MMDDYY10.;
RUN;
PROC SORT DATA=WORK.FLIGHTBUSIEST OUT=WORK.FLIGHTBUSIEST_SORTED;
BY HOUROFTIME ;
RUN;
PROC FREQ data=WORK.FLIGHTBUSIEST_SORTED;
TABLE DATE*AIRLINES_NAME*HOUROFTIME / NOPRINT NOCOL NOCUM NOPERCENT NOROW OUT=WORK.AIRLINESTRAFFIC;
RUN;
PROC SORT DATA=WORK.AIRLINESTRAFFIC OUT=AIRLINESTRAFFICSORTED;
BY DESCENDING COUNT;
RUN;
DATA VDIR.AIRLINESTRAFFICSORTED;
SET WORK.AIRLINESTRAFFICSORTED;
RUN;

PROC MEANS DATA=AIRLINESTRAFFICSORTED MAX NOPRINT ;
CLASS DATE AIRLINES_NAME;
VAR HOUROFTIME COUNT;
OUTPUT OUT=AIRLINESBUSIESTTRAFFIC;
RUN;
/*Question 7*/
/*7.1*/
data work.delayinfo(KEEP= DATE ORIGIN DEST DEP_STATUS);
set vdir.flightsnew(where=(origin="JFK"));
RUN;
PROC FREQ DATA=WORK.delayinfo;
TABLES DEP_STATUS / NOCUM NOFREQ NOCOL;
TITLE "JFK DEPARTURE(DELAY/ARRIVAL)";
RUN;
/*7.2*/
data work.delayinfo;
set vdir.flightsnew(KEEP=DATE ORIGIN DEST SCH_DEP_TIME DEPA_TIME H DEP_STATUS);
RUN;
DATA WORK.delayinfoTIMEINTERVAL;
SET WORK.delayinfo;
IF SCH_DEP_TIME>'00:00'T AND SCH_DEP_TIME<='03:00'T THEN INTERVAL="0-3 AM";
IF SCH_DEP_TIME>'03:00'T AND SCH_DEP_TIME<='06:00'T THEN INTERVAL="3-6 AM";
IF SCH_DEP_TIME>'06:00'T AND SCH_DEP_TIME<='09:00'T THEN INTERVAL="6-9 AM";
IF SCH_DEP_TIME>'09:00'T AND SCH_DEP_TIME<='12:00'T THEN INTERVAL="9-12 NOON";
IF SCH_DEP_TIME>'12:00'T AND SCH_DEP_TIME<='15:00'T THEN INTERVAL="12-3 PM";
IF SCH_DEP_TIME>'15:00'T AND SCH_DEP_TIME<='18:00'T THEN INTERVAL="3-6 PM";
IF SCH_DEP_TIME>'18:00'T AND SCH_DEP_TIME<='21:00'T THEN INTERVAL="6-9 PM";
IF SCH_DEP_TIME>'21:00'T AND SCH_DEP_TIME<='00:00'T THEN INTERVAL="9-12 PM";
IF INTERVAL="" THEN DELETE;
RUN;
PROC FREQ DATA=WORK.delayinfoTIMEINTERVAL;
TABLES INTERVAL*ORIGIN / NOCUM NOFREQ NOPERCENT NOROW OUT=WORK.DELAYPERORIGIN;
RUN;
PROC SORT DATA=DELAYPERORIGIN;
BY DESCENDING COUNT;
RUN;
/*7.3*/
data work.delay_ARRIVAL_info(WHERE=(ARR_STAT="DELAY ARRIVA"));
set work.flightsnew(KEEP=DATE ORIGIN DEST ARR_STAT);
RUN;
PROC FREQ DATA=WORK.delay_ARRIVAL_info;
TABLES DEST*ARR_STAT/ NOCUM NOFREQ NOPERCENT NOROW OUT=WORK.DELAY_PER_DESTINATION;
RUN;
/*SORTING % DELAY IN ARRIVAL TO KNOW HIGHEST DELAYS*/
PROC SORT DATA=WORK.DELAY_PER_DESTINATION OUT=WORK.DELAY_PER_DESTINATION_SORTED;
BY DESCENDING PERCENT;
title "Destination with Highest delays";
RUN;

/*Question 8*/
/*8.1*/
PROC SQL;
CREATE TABLE WORK.flight_with_weather AS
SELECT * FROM work.flightsNEW A 
left join WORK.weathernew B
ON A.date=b.date and A.origin=B.origin and A.h=b.h;
QUIT;
/*8.2*/
proc corr data=work.flight_with_weather;

proc means data=work.flight_with_weather(where=(dep_status="DELAY DEPARTURE")) mean N NWAY;
classes dep_status m;
var temp	dewp	humid	wind_dir	wind_speed	wind_gust	precip	pressure	visib;
output out=work.flight_with_weathermeans;
run;
data work.flight_with_weathermeansFILTERED(keep=dep_status m  _stat_ temp	dewp
humid	wind_dir	wind_speed	wind_gust	precip	pressure	visib _FREQ_);
set work.flight_with_weathermeans(where=(_STAT_="STD" AND (DEP_STATUS<>"" AND M<>.) ));
TITLE "AVERAGE OF WEATHER CONDITION PARAMETERS GROUPED WITH DEPARTURE DELAY AND MONTH";
run;
/*8.3*/
/*Correlation of delays with average weather parameters*/
/*As per the Data the Delays mostly correlates with Temperature, Dewpoints and visibility*/
/*Refer CSV file for same*/
/*Renaming Weather Variables*/
DATA WORK.flight_with_weathermeansFILTERED;
SET WORK.flight_with_weathermeansFILTERED(RENAME=(temp	=Temperature_F
dewp	=	Dewpoint_F
humid	=	Relative_humidity
wind_dir	=	Wind_direction
wind_speed	=	wind_speed_mph
wind_gust	=	gust_speed_mph
precip	=	Preciptation_in_inches
pressure	=	Sea_level_pressure
visib	=	Visibility
));
run;

/*Question 9*/

/*9.1*/

/*importing planes datasets*/
PROC IMPORT DATAFILE='/folders/myfolders/SAS Master Case study 1 - Updated files (1)/SAS Case study 1 files/planes.csv'
	DBMS=CSV
	OUT=WORK.PLANES;
	GETNAMES=YES;
RUN;
PROC IMPORT DATAFILE='/folders/myfolders/SAS Master Case study 1 - Updated files (1)/SAS Case study 1 files/flights.csv'
		    OUT=WORK.FLIGHTS
		    DBMS=CSV
		    REPLACE;
GETNAMES=YES;
RUN;
data vdir.flightsNEW;
set work.flightsNEW;
run;
DATA WORK.PLANESNEW;
SET WORK.PLANES;
TAILNUM=PLANE;
RUN;
/*Sorting flightsNEW by tailnumber*/
proc sort data=vdir.flightsNEW out=work.flightsNEW;
by tailnum;
run;
/*merging flights and planes datasets*/
DATA WORK.Flight_with_Planes;
MERGE work.flightsNEW(IN=A) WORK.PLANESNEW(IN=B);
BY TAILNUM;
IF A and b;
FUEL_CC=ROUND(FUEL_CC,.01);
FORMAT FUEL_CC DOLLAR10.2;
RUN;
/*Sorting dataset by fuel_cc*/
proc sort data=WORK.Flight_with_Planes(where=(manufacturing_year<>. and FUEL_CC<>.)) out=work.fuelcc_manuf_year(keep=tailnum manufacturing_year manufacturer model fuel_cc );
by descending FUEL_CC;
run;
/*As per data set Oldest plane has the highest fuel consumption cost*/


/*9.2*/
data work.Flight_with_Planes_filtered;
set work.flight_with_planes(where=(FUEL_CC<>. and type<>"" and engine<>""));
run;
/*Corelating No. of Seats with Fuel Consumption*/
proc means data=work.flight_with_planes_filtered mean nway;
class seats;
var fuel_cc;
output out=work.seats_fuel_cc;
run;
data work.seat_fuel_cc_filtered(drop= _type_);
set work.seats_fuel_cc(where=(_STAT_="MEAN"));
run;
proc corr data=work.seat_fuel_cc_filtered spearman plots= scatter;
var seats fuel_cc;
run;
/*No. of seats strongly correlates with fuel_cc. Fuel consumption tends to increase with increase in number of seats*/.

/*Corelating plane type with Fuel Consumption*/
proc means data=work.flight_with_planes_filtered std nway;
class type;
var fuel_Cc;
run;
/*Fixed wing multi engine has highest fuel consumption*/

/*corelating no. of engines with fuel_Cc*/
proc means data=work.flight_with_planes_filtered nway;
class engines;
var fuel_cc;
output out=work.engines_fuel_cc;
run;
data work.engines_fuel_cc(drop= _type_);
set work.engines_fuel_cc(where=(_STAT_="MEAN"));
run;
proc corr data=work.engines_fuel_cc plots=scatter;
var engines fuel_cc;
run;
/*There is Very weak Correlation between no. of engines and fuel consumption*/

/*Corelating type of engine type with Fuel consumption*/
proc means data=work.flight_with_planes_filtered nway;
class engine;
var fuel_cc;
output out=work.enginetype_fuel_cc;
run;
data work.enginetype_fuel_cc(drop= _type_);
set work.enginetype_fuel_cc(where=(_STAT_="STD"));
run;
proc corr data=work.enginetype_fuel_cc plots=scatter;
var _freq_ fuel_cc;
run;/*NO correlation between engine type and fuel consumption*/

/*Question 10*/
proc freq data=vdir.flightsnew(where=(dep_Status="DELAY DEPARTURE"));
tables Date*h*dep_Status / nocol nocum nopercent norow noprint out=date_hour_dep_status;
run;

proc sort data=date_hour_dep_status out=vdir.date_hour_dep_status;
by date;
run;
proc means data=vdir.date_hour_dep_status nway;
class date dep_status;
var count;
output out=VDIR.date_dep_status(where=(_STAT_="MEAN"));
run;
PROC MEANS DATA=VDIR.date_dep_status NWAY;
VAR COUNT;
OUTPUT OUT=WORK.MEAN_OF_MEAN(WHERE=(_STAT_="MEAN"));
RUN;
proc plot data=work.date_hour_dep_status;
plots count*h="*";
run;
/*Average Delay increases in evening then drastically decreases in late hours*/