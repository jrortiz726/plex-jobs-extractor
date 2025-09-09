## Creating an API request
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

On this page
Decide which URL to use
Customer-specific URLs
Create an authorization header
Set the HTTP method
Set the HTTP headers
Building the request body
