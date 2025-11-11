Admin User = RA-Demo.Admin 
Admin Company = RA-Discrete 
Admin PW = 123plex123 
  
JSON Datasource User = RockwellProcessWs@plex.com 
JSON Datasource User PW = 5915cbc-431a-4 
JSON Datasource Documenation = https://ra-process.on.plex.com/Platform/CustomerDataSourceManager

Source URL: https://docs.plex.com/pmc/en-us/integration/data-source-api/data-source-api.htm

e Data Source API (Application Programming Interface) allows you to integrate other software in your enterprise with Plex by making requests to a set of data sources.

Data sources available in the Data Source API are like wrappers for stored procedures that perform one or more related tasks. Calling a data source through the API is like executing a stored procedure and is recorded as a data source transaction.

Obtain access credentials to call a data source
To request a new developer account to access the Data Source API, contact Plex Customer Care.

Note
If you have a developer account for using Plex Classic Web Services, use those credentials for the Data Source API.

Definitions
credentials
A combination of a username and password used to authorize API requests.
data source
A wrapper for a stored procedure that performs one or more related tasks. Calling a data source through the API is like executing a stored procedure and is recorded as a data source transaction
environment
Data sources can be called in two environments:

Production: the live environment where your organization's data is updated in real time.
Test: a copy of the live environment that is used for trying out new features or testing functionality, including API requests.
global allow
A field on a data source that is managed by Plex and indicates if a web service user account can be granted access to execute it.
json (JavaScript Object Notation)
A standard way to format data that is shared between two systems. It is easy for humans to read and write, and can be easily parsed by computers.
self serviceable (data source)
A data source that can be granted access to a web service user account to execute through HTTP or within Process Flows.
transaction
A combination of a single request to a data source and the response returned.

Identify a data source
Use the Data Sources screen to search for and view the available data sources in Plex and their corresponding IDs.

Open the Data Sources screen.
Adjust the filters, and click Search.
Locate the data source in the Data Source Name column and note the ID in the ID Column. This is the ID to use in the request when calling this data source using the HTTP protocol.

ID - The ID of the data source; use in the request when calling this data source.
Data Source Name - Name of the Data Source, with the corresponding database name in parenthesis ( ). For example, Action_Type_Get (Part), where Action_Type_Get is the name of the Data Source, and Part is the name of the database.
Data Source Type - Stored Procedure is the most common type.
Inputs - The number of inputs that are part of the request.
Outputs - The number of outputs that are part of the response.
Module Group - The module group to which the data source belongs.
Module Name - The name of the module to which the data source belongs.
Self Serviceable - If checked, indicates that a web service user account within your tenant can be granted access to execute the data source.
Global Allow - If checked, indicates that the data source has been preapproved to be self serviceable if its assigned module is turned on in the tenant.
Select a row and click Details on the Action Bar to view additional information.
The Data Source Detail screen appears with information for the data source that you selected, including specific input and output information.
Grant a web service user access to a data source
To grant access to a data source, one of the following scenarios must be true:

Scenario 1	Scenario 2
Your tenant must already have a web service account granted access to that data source.
The data source is Plex-owned, not community-owned.
The data source must have the Global Allow flag turned on.
The data source must have a module (and module group) assigned to it.
Your tenant must have the same module turned on.
Open the Web Service Access By User Account screen.
Adjust the filters, including selecting a User Account.
Click Search.
In the grid, locate the data source(s) to which the specified user account in the filter should have access.
Click the box in the Granted column.
Click the Apply button at the bottom of the page.
Request access to a data source
If you see a data source on the Data Sources screen but not on the Web Service Access By User Account, contact Plex Customer Care to review your requirements.

Use the Data Sources screen to identify the Data Sources to which you need access.
Have your Plex Champion submit a case to Plex Customer Care to request access to the Data Sources.
Request web service user account
Contact Plex Support to request a new web service user account to execute data sources through the HTTP protocol or within Process Flows.

Configuration requirements
Security actions
Application name	Action title	Details
Customer Data Sources	Data Sources	View a list of available data sources.
Search Data Sources	Search available data sources.
Data Source Detail	View the details of a data source, including names of inputs used in the request and names of outputs in the response.
Web Service Access By User Account	Web Service Access by User Account	View a list of Self Serviceable data sources.
Web Service Access by User Account - Search	Search Self Serviceable data sources.
Web Service Access By User Account - Update	Grant access to a data source to a specified user account.

Using metadata endpoints to get data source information
Updated: 2022-03
Use the following endpoints to retrieve metadata for data sources from Plex.

Note: Higher level text formats are all returned as strings, including XML and HTML.

Date strings always include time data while time strings are more like stop watch values.
Retrieve a list of data sources to which you have access
Request
GET /api/datasources/search?name=

Response
The response will be a list of all of the data sources to which your user account has access. Contact Plex to request access to other data sources.

Search for data source by name
Search for data sources that contain a match on the name string.  This returns an array of simplified header records.

Request
GET /api/datasources/search?name={contains-name}

Response
[
  {
    "id": {id},
    "name": "{name}",
    "description": "{description}"
  }
]
Metadata
Get a full object graph of metadata for a specific data source including input and output collections.  Returns a deep object graph of all public metadata for a data source.

Request
GET /api/datasources/{id}

Response
{
  "id": {id},
  "name": "{name}",
  "inputs": [
    {          
      "name": "{name}",
      "type": "{type}",
      "required": true/false,
      "nullable": true/false
    }
  ],
  "outputs": [
    {          
      "name": "{name}",
      "type": "{type}",
      "nullable": true/false
    }
  ],
  "columns": [
    {          
      "name": "{name}",
      "type": "{type}",
      "nullable": true/false
    }
  ]
}
Type reference
Type	Description
string	
A string.

bool	
A boolean literal value.

true/false

date	
A date string formatted with a commonly used ISO 8601 format string.

"YYYY-MM-DDThh:mm:ss.fffffffZ"

Note
Currently, all date values are normalized to UTC, as indicated by the trailing 'Z' used in place of TZ offset.

time	
A time string using a commonly understood format.

"hh:mm:ss.fffffff"

uuid	
A UUID or GUID string formatted as lower-case with hyphens but without the redundant curly braces.

"00000000-0000-0000-0000-000000000000"

blob	
A base64 string for raw binary data.

int16	A 16-bit signed integer.
int32	A 32-bit signed integer.
int64	A 64-bit signed integer.
float	A 16-bit floating point number.
double	A 32-bit floating point number.
decimal	A 64-bit floating point number.

Creating an API request
Updated: 2022-03
Decide which URL to use
Cloud URLs
If your organization uses a cloud.plex.com URL, use the following URLs:

Environment

Host

URL Path

Plex Production

https://cloud.plex.com

/api/datasources/{id}/execute

Plex Test

https://test.cloud.plex.com

/api/datasources/{id}/execute

Customer-specific URLs
If your organization has a URL using your tenant code and on.plex.com, use the following URLs:

Environment

Host

URL Path

Plex Production

https://{PCN_Code}.on.plex.com

/api/datasources/{id}/execute

Plex Test

https://{PCN_Code}.test.on.plex.com

/api/datasources/{id}/execute

Create an authorization header
Create a BASIC Authorization header using your credentials from Plex. Contact Plex Customer Care to request a developer account.

The following is a sample header using UTF-8/Base-64 encoding:

var credentials = username + ":" + password;                  // Your user account credentials
var bytes = utf8GetBytes(credentials);                        // Standard library in most languages
var encodedCredentials = base64Encode(bytes);                 // Standard library in most languages
var authorizationHeaderValue = "Basic " + encodedCredentials; // Authorization header value
Set the HTTP method
Each call to the Data Source API requires the POST method.

Set the HTTP headers
Construct the HTTP header using the following guidelines for the request and response.

Header

Value

Status

Description

Accept

application/json

Optional

Defines JSON as the response media type. This is implied by the Content-Type header, however Plex does not support alternative response types at this time.

Accept-Encoding

gzip, deflate

Recommended, Multiple

Defines a list of acceptable encodings that can be used when sending the HTTP response from the server to the client.

Plex recommends using gzip,deflate for its efficiency.

Authorization

Basic {encodedCredentials}

Required

See the Authorization Header.

Content-Type

application/json; charset=utf-8

Required

Defines JSON as the response media type. This is required.

The charset is implied as UTF-8.

When using this Content-Type, the response will be in the same media type.

Building the request body
The Data Source API uses JavaScript Object Notation (JSON).

Default Formatting
URL
POST /api/datasources/1791/execute

Request
{
  "inputs": {
    "Part_No": "1234"
  }
}
Response - Raw
{"outputs":{},"tables":[{"columns":["Part_Key","Part_No_Revision","Name","Part_Status","Old_Part_No"],"rows":[[8265836,"1234-ABC","Example part 1234 rev ABC","Production",""]]}],"rowLimitedExceeded":false,"transactionNo":"1234567890"}
Response - JSON Viewer
       {
  "outputs": { },
  "tables": [
    {
      "columns": [
        "Part_Key",
        "Part_No_Revision",
        "Name",
        "Part_Status",
        "Old_Part_No",
      ],
      "rows": [
        [
          8265836,
          "1234-ABC",
          "Example part 1234 rev ABC",
          "Production",
          ""
        ]
      ]
      "rowLimitedExceeded": false,
    }
  ],
  "transactionNo": "1234567890"
}
    
Optional Formatting
You can set the format query string parameter to 2 to use different request and response formats.

Request
POST /api/datasources/1791/execute?format=2

Request
 {
  "Part_No": "1234"
}
Response
{
  "outputs": {},
  "rows": [
    {
      "Part_Key": 8265836,
      "Part_No_Revision": "1234-ABC",
      "Name": "Example part 1234 rev ABC",
      "Part_Status": "Production",
      "Old_Part_No": ""
    }
  ],
  "rowLimitedExceeded": false,
  "transactionNo": "1234567890"
}
Optional Indentation
Specify pretty=true on the query string of your execution requests.

Without the option, or when specifying "pretty=false", the response body will contain compact JSON without whitespaces between JSON tokens. This is the default behavior.

With the option, the response body will use newlines and 2-space indent between JSON tokens.

Note
This formatting feature can be used with the default request/response format or any explicit override. It is independent of structure.

Request
POST /api/datasources/1791/execute?format=2&pretty=true

Dates and times in API calls
Updated: 2022-03
Use UTC (Coordinated Universal Time) or UTC+Offset standard data and time formats.

Format string
Use the following format for any date or time input parameters:

YYYY-MM-DDThh:mm:ss.fffffffZ
Symbols
Format component	Notes
YYYY	Four-digit year (required)
MM	Two-digit month (01 through 12, required)
DD	Two-digit day of month (01 through 28/29/30/31, required)
T	Constant char separator between date and time components (required)
hh	Two-digit hour (00 through 23, required)
mm	Two-digit minute (00 through 59, required)
ss	Two-digit second (00 through 59, optional)
fffffff	Fractional digits of second (0 to 7 digits, optional)
Z	Constant char designation for UTC a.k.a. "Zulu Time" (required)
The follow also applies:

The seconds component may be excluded.

Fractional digits of seconds may be excluded, but when included may not exceed 7 digits. This applies to both inputs and outputs.

Trailing zeros are allowed, but are not required

The following are not supported:

Dates without times

Times without a time zone

Fractional digits for anything except seconds

Using a blank space instead of the 'T' character for date time separator

Range of accuracy
1999-12-31T12:34Z
1999-12-31T12:34:56Z
1999-12-31T12:34:56.1Z
1999-12-31T12:34:56.12Z
1999-12-31T12:34:56.123Z
1999-12-31T12:34:56.1234Z
1999-12-31T12:34:56.12345Z
1999-12-31T12:34:56.123456Z
1999-12-31T12:34:56.1234567Z

HTTP status codes
Code

Status

Description

200

Success

The data source execution was successful. There were no validation issues, logical, or unhandled errors, and a successful response format was used.

400

Bad Request

There was something wrong with the request, such as invalid JSON format or another type of validation error.

401

Not Authorized

There is a problem with your user account or credentials. Contact Plex Customer Care for help with your developer account.

403

Forbidden

Security or access control error. Use the details in the response error to troubleshoot.

405

Method not allowed

The method specified in the request is not allowed for the requested resource.

500

Internal Server Error

An internal server error. Try the request again or contact Plex Customer Care.

Error Codes
Code

Message

CUSTOMER_ACCESS_DENIED

You do not have access to customer '{id}'.

You do not have access to one or more customers in delimited list '{value}'.

DATA_SOURCE_ACCESS_DENIED

You do not have access to data source '{id}'.

DATA_SOURCE_API_ACCESS_DENIED

You do not have access to the Data Source API.

DATA_SOURCE_INPUT_FORMAT_INVALID

Input '{inputName}' for data source '{dataSourceName}' is not a valid delimited list of customers.

Input '{inputName}' for data source '{dataSourceName}' is not a supported ISO UTC date format. Value: {value}

DATA_SOURCE_INPUT_NOT_ACCESSIBLE

Input '{inputName}' for data source '{dataSourceName}' is controlled automatically and is not accessible by users.

DATA_SOURCE_INPUT_NOT_FOUND

Input '{inputName}' for data source '{dataSourceName}' was not found. Please verify the name matches an input in the existing metadata.

DATA_SOURCE_INPUT_NOT_NULLABLE

Input '{inputName}' for data source '{dataSource.Name}' does not allow null values.

DATA_SOURCE_INPUT_REQUIRED

Input '{inputName}' for data source '{dataSourceName}' is required but was not specified.

DATA_SOURCE_INPUT_TYPE_MISMATCH

Input '{inputName}' for data source '{dataSourceName}' is configured as type '{typeName}' and the specified value could not be converted safely.

DATA_SOURCE_NOT_FOUND

Data source '{id}' was not found.

DATA_SOURCE_OBJECT_NOT_FOUND

The run-time object for data source '{id}' was not found.

METHOD_NOT_ALLOWED

The method specified in the request is not allowed for the requested resource.

REQUEST_PROCESSING_ERROR

An unexpected error occurred while processing your request. If the problem persists, please contact Plex Customer Care for assistance.

USER_ACCESS_DENIED

You do not have access to user '{id}'.

Sample Data Source API requests and responses
Updated: 2022-03
Note
Metadata names are case-sensitive. The property names must match the metadata exactly. The value must be compatible with the data type defined in metadata for the input. If either do not match you will receive an error response containing the corresponding validation error(s).

Example with a table in the response
The following example is a request that uses the data source Parts_Picker_Get3. The request must include the required inputs, however you may specify as many optional inputs as necessary.

Request Format
{
				"inputs" : {
				"Part_Type" : "Bolt",
				"Active_Flag" : true
				}
		}
Field

Type

Description

inputs

Dictionary (object)

A dictionary object for all inputs containing property names and values, where the property name is the input name.

Response Format (Successful)
The following is an example of a successful response format, using the Parts_Picker_Get3 data source as an example:

{
  "outputs" : { },
  "tables" : [
    {
      "columns" : [
        "Part_Key",
        "Part_No",
        "Revision",
        "Part_No_Revision",
        "Name",
        "Part_Type",
        "Part_Status",
        "Note",
        "Revision_Effective_Date"
      ],
      "rows" : [
        [
          1857393,
          "82695",
          "AAA",
          "82695-AAA",
          "7/8-9x16 Hex Bolt, rev AAA",
          "Bolt",
          "Production",
          "Copied from part 7/8 hex bolt template",
          null
        ],
        [
          1859737,
          "82695",
          "ABC",
          "82695-ABC",
          "7/8-9x16 Hex Bolt, rev ABC",
          "Bolt",
          "Production",
          "Copied from part 7/8 Hex Bolt",
          "2018-07-16T20:00:00.000Z"
        ],
      ],
      "limitExceeded" : false
    }
  ],
  "transactionNo" : "1234567890"
}
Field

Type

Description

outputs

Dictionary

(object)

A dictionary object for all direct outputs containing property names and values, where the property name is the output name.

tables

Table [ ]

(array)

Contains the collection of data tables returned by the data source.

There will only be one data table in the array.

 	
Table

(object)

An object representing a data table. A table consists of columns and rows, both are arrays with matching ordinals. Columns are sometimes also referred to as row outputs. Rows are a two-dimensional array of data values matching the order of columns.

transactionNo	string	
The transaction number to use for logging and traceability. This value can be used to look up a single request on the Data Source API Transactions screen.

Example with only direct outputs (no table)
Request format
This example uses Part_Name_Output_Get .

{
				"inputs" : {
				"Part_Key" : 1859737
				}
		}
Response format
{
				"outputs" : {
				"Name" : "7/8-9x16 Hex Bolt, rev ABC"
				},
				"tables" : [ ],
				"transactionNo" : "1234567890"
		}
Note
There are no data tables in this procedure, only a single output, which is why the tables array are empty.

Examples of responses with errors
Access Denied Error (Response Format)
The following is an example of a response with the error you will receive if you try to call a data source to which you do not have access.

{
  "errors" : [
    {
      "code" : "DATA_SOURCE_ACCESS_DENIED",
      "message" : "You do not have access to data source '123'."
    }
  ],
  "transactionNo" : "1234567890"
}
Input Not Found Error (Response Format)
The following is an example of a response with the error you will receive if you specify an input that does not exist.

{
  "errors" : [
    {
      "code" : "DATA_SOURCE_INPUT_NOT_FOUND",
      "message" : "Input 'This_Is_Not_A_Real_Input' for data source 'My Example Data Source' was not found.  Please verify the name matches an input in the existing metadata."
    }
  ],
  "transactionNo" : "1234567890"
}
Field

Type

Description

errors

Error [ ]

(array)

Contains the collection of direct outputs returned by the data source API.

 	
Error

(object)

An object representing a single error containing a code and a message.

The code is defined as a string constant that will not change.

The message describes the nature of the error and may include contextual values. You should NOT implement any string parsing or regex expressions to test for messages as they are subject to change.

transactionNo	string	
The transaction number to use for logging and traceability. This value can be used to look up a single request on the Data Source API Transactions screen.

Viewing Data Source API transactions
Updated: 2022-03
Use the Data Source API Transactions screen to view a log of calls made to data sources for a specific tenant.

Access the Data Source API Transactions screen
Adjust the filters, and click Search.
The screen displays a list of all calls made through the entity that you are logged in to.
