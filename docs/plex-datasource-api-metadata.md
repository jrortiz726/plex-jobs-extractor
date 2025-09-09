## Using metadata endpoints to get data source information

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