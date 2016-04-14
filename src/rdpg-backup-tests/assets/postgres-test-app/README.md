# postgres-test-app

##Endpoints:
All calls respond with "SUCCESS" as their first line if the call performs the behavior as expected, and will return "FAILURE" as their first line otherwise.

###GET '/'  
Displays the current timestamp if connecting to the database and creating a key-value table was successful.

###GET '/timestamp'  
Alias for GET '/'

###GET '/ping'  
Returns "SUCCESS" if this endpoint is reachable; i.e. if the app
is running and receiving connections.

###GET '/services'  
Displays the VCAP\_SERVICES environment variable of the
application instance. It is considered to be a failure if the
VCAP\_SERVICES environment variable does not exist in the
application's environment.

###GET '/uri'  
Displays the connection uri for the Postgres database bound to the
app. Successful if such a uri exists and the app can access it. Failure otherwise.

###GET '/uri/username'  
Displays the username found in the uri. Successful if uri exists and contains a username.  

###GET 'uri/password'  
Displays the password found in the uri. Successful if uri exists and contains a password.  

###GET 'uri/location'  
Displays the location found in the uri (i.e. the ip address or url). Successful
if the uri exists and contains a location.  

###GET 'uri/port'  
Displays the connection port as found in the uri. Successful if the uri exists
and contains a connection port.  

###GET 'uri/dbname'  
Displays the name of the database found in the uri. Successful if the uri exists
and contains a database name.  

###GET 'uri/params'  
Displays the parameters of the connection string, as they are url-encoded. Successful
if the uri exists and has parameters specified.

###POST '/exec'  
Takes, as the body, an parameter named "sql" which is equal to the SQL query to be executed on the app's database. An example format from `curl` is as follows:

	curl -X POST <appurl>/exec -d "sql=INSERT INTO test VALUES ('foo', 'bar');"
Note that double-quotes are used to delimit the -d argument of curl so that single-quotes can be freely used in the SQL query.

The response to this endpoint will be the expected SUCCESS/FAILURE
message, and following that, starting on a new line, returned rows
(if any) will be given in JSON format, where the output is a JSON
array of rows, and each row is in turn represented as a JSON array
of values. The values are all encoded as strings, seemingly as a
limitation of the pg library.

Note that, due to limitations of the underlying pg api, if the input "query" is actually multiple queries concatenated with semicolons, while all of them will be executed, only the rows of the last query (if any) will be returned.
