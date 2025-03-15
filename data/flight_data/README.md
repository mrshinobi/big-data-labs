# On-Time Flight Data #

The attached data set contains the ontime flight information for U.S. flights for the month of April 2014. This data was obtained from the [Research and Innovative Technology Administration (RITA)][rita] and measures the ontime performance of airlines flying from U.S. cities. 

The fields in the data set (in columnar order in the TSV are as follows):

* Flight Date: Date of flight (yyyy-mm-dd)
* Airline: A US DOT identification for airline
* Carrier: The text description of the airline
* Origin: The airport code of the origination airport
* Destination: The airport code of the destination airport
* Departure Time: Actual departure time (local time: hhmm)
* Departure Delta: Difference in minutes between scheduled time and depature; Negative numbers indicate early depatures
* Departure Delay: Difference in minutes between scheduled and actual departure time. Early departures set to 0.
* Arrival Time: Actual arrival time (local time: hhmm)
* Arrival Delta: Difference in minutes between scheduled time and arrival Negative numbers indicate early arrivals.
* Arrival Delay: Difference in minutes between scheduled and actual arrival time. Early arrivals set to 0.
* Cancelled: 1 if the flight was canceld, 0 otherwise
* Distance: Distance between origin and destination in miles  
* Carrier Delay: Carrier Delay, in Minutes
* Weather Delay: Weather Delay, in Minutes
* NAS Delay: National Air System Delay, in Minutes
* Security Delay: Security Delay, in Minutes
* Late Aircraft Delay: Late Aircraft Delay, in Minutes

Each field is separated by a tab (`\t`) character.

The 3 correlary files include lookup tables:

* airlines.tsv: Airline ID Codes and Descriptions
* carriers.tsv: Carrier Code and Descriptions
* cancellation_reasons.tsv: Cancellation Reason Codes and Descriptions

Data statistics:

* Elapsed time period: 1 month (April 1, 2014 - April 30, 2014)
* Number of flights (rows): 483,499
* File size: 31.6 MB

[rita]: http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time "Bureau of Transportation Statitics"
