## PlexMES API basic configuration

Get Started

Developer Portal Account
To access the Plex APIs in the developer portal, you must have Plex IAM activated.

Logging in to the developer portal
Work with your Plex Champion to file a support ticket at https://plexcustomercare.force.com/ by providing all necessary details
Upon confirming the case is resolved, access https://developers.plex.com/
Enter your company name in the company code section. Click Next
Enter your company email ID. Click Next
Enter your password. Click Next

Obtaining API credentials
To obtain your API credentials, you must first set up a developer portal account following the steps above. Only users with developer portal accounts can create API keys.

To obtain your API credentials (Consumer Key and Consumer Secret), create an application in the Plex Developer Portal.

Access the Plex Developer Portal at https://developers.plex.com/.
Click on your account name and select My Apps.
Click Add a New App.
On the Add App screen, enter a name for your app.
For Product (also known as API categories), select the API products you want to access to.
Click Create App.
The Consumer Key and Consumer Secret is generated for your application and appears on the screen.


Request Headers
Subscription Key Header
All Plex API requests require a valid subscription key header. The request header X-Plex-Connect-Api-Key is a subscription key that provides access to the API. You can obtain your API key through the Plex Developer Portal.

If you do not include the subscription key header in an API request or provide an invalid subscription key, you receive the following response:

{
   "code": "REQUEST\_NOT\_AUTHENTICATED"
   "message": "The request could not be authenticated."
}

Tenant Id Header / (Multi PCN Access)
Plex API requests are performed in the context of a single default tenant (also called an entity or PCN). The request header X-Plex-Connect-Tenant-Id can be used to execute the request for a tenant other than your default tenant. The header value must be a tenant ID.


Customer Id Header
The request header X-Plex-Connect-Customer-Id can be used to execute the request for a different tenant other than your default tenant. The header value must be a PCN value.

If you do not have access to the requested tenant, you will receive the following response:

{
   "code": "REQUEST\_NOT\_AUTHENTICATED"
   "message": "The request could not be authenticated."
}

URL structure
Managed endpoints have this structure:

  https://[tier.]connect.plex.com/{collection}/{version}/{resource}[/{resource id}]?[filters]
“Tier” represents the Product or Test environment.

Example for Production Environment:

   https://connect.plex.com/mdm/v1/employees?lastName=smith

Example for Test Environment:

   https://test.connect.plex.com/mdm/v1/employees?lastName=smith

List Query String Parameters
GET method APIs use query string / URL Parameters.

To use multiple values for a list query string parameter, construct the URL with multiple instances of the query string parameter and its value pair.
For example, a query string parameter ID that can accept a list of values could have a URL for the request that looks like this:

   https://?Id=value1&Id=value2&Id=value3. 

In this example, three different values are provided for the Id query string parameter, and the request uses the three parameters that are listed. 

Methods such as PUT and POST use Request Body parameters.


Call the APIs
Using "Try-It" from the developer portal
If you have a Plex API developer account, you can test Plex APIs directly within the Plex developer portal using Try It. This functionality only works for GET endpoints, and not any other API methods.

Log in to the Developer Portal and go to the APIs page.
Click the API operation that you would like to test.
Click Try It to view its details, such as the resource URL and the request body.
After the Request URL, an Apps Dropdown appears that has a list of apps created by you. Select one of the following.
App (the Consumer Key of this app is retrieved to call this API).
Update (if applicable) Request Parameter(s).
Update (if applicable) Request Body.
Click Send request.
The API request sends and the Response Tab updates with the response.
After a successful response, you can use the API. Update your case in ServiceCloud to confirm you can access the API.  

Calling APIs from a third-party environment
Users can use third-party apps such as postman, Thunder client, paw, Testfully for calling methods other than "GET" API endpoints or can use the command line as well to call on API. 

Users need to ensure that the following details need to be mentioned in the HTTP request header;

In the Developer Portal, go to the APIs page.
Click the API operation that you would like to test.
Click a resource to view its details, including the resource URL and the request body.
After the Request Body, in the API Key field, click Set.
For Name, type X-Plex-Connect-Api-Key.
For Value, type your Customer Key. (You can retrieve this from the App that you created. See Obtain API credentials.)
Select Header.
Click Ok.
Click Send this request.
The API request is sent. The page will update with the response.
After a successful response, you are ready to use the API.

Dates and Times
Coordinated Universal Time (UTC)
Use UTC or UTC+Offset standard data and time formats.


Format String 
Use the following format for any date or time input parameters:

YYYY-MM-DDThh:mm:ss.fffffffZ 

Symbols
YYYY   -->   Four-digit year (required)

MM     -->   Two-digit month (01 through 12, required)

DD      -->   Two-digit day of the month (01 through 28/29/30/31, required)

T         -->   Constant char separator between date and time components (required)

hh      -->   Two-digit hour (00 through 23, required)

mm    -->   Two-digit minute (00 through 59, required)

ss       -->   Two-digit second (00 through 59, optional)

fffffff  -->   Fractional digits of a second (0 to 7 digits, optional)

Z        -->   Constant char designation for UTC a.k.a. "Zulu Time" (required)  

The following also applies:

Fractional digits of seconds are optional and cannot exceed 7 digits. This applies to both inputs and outputs.
Trailing zeros are optional. 
The following is not supported:

Dates without times.
Times without a time zone.
Fractional digits for anything except seconds.
Using a blank space instead of the 'T' character for date-time separator.

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

Error Codes
200 - Success
Response code starting with 200 means query has executed successfully. 

400 - Malformed request
400 errors generally indicate that the body of the request does not match the resource being requested. 

400 - Validation failure
One or more of the parameters use an incorrect format or are omitted from the request.

401 - Request not authenticated
A 401 error can occur when you try to access the system using an expired API key, an invalid API key, or without an API key at all.
If you receive this error, verify that the X-Plex-Connect-Api-Key header is specified on the request and that the key matches the value in the Plex Developer Portal.

403 - Forbidden
A 403 forbidden client error status response code indicates that the server understood the request but refuses to authorize it. It means it's a resource that you're not allowed to access. This status is similar to 401, but in this case, re-authenticating makes no difference.

404 - Resource not found
A 404 error can occur when your application attempts to access Plex functionality using an incorrect request URL or if no data exists to return.

500 - Request processing error
500 errors are unexpected. If you can reproduce the error, submit a support ticket to Plex. Include the steps to duplicate the issue, but do not include client secrets, passwords, or subscription keys.


Glossary
API (Application Programming Interface): APIs enable secure access to enterprise data assets and processes. They are usually in the form of REST or SOAP web services. APIs are the building blocks for app creation integrations.
API Version: The version of an API interface or a group of API interfaces if they are defined together. An API version is often represented by a string, such as "v1", and appears in API requests and Protocol Buffers package names.
API Method: An individual operation within an API Interface. It is represented in Protocol Buffers by an RPC definition and is typically mapped to a function in an interface.
API Request: A single invocation of an API Method. It is often used as the unit for billing, logging, monitoring, and rate-limiting.
Client Applications: The apps that developers build. When these applications call APIs from the API Directory, they are considered a client application of the API Directory.
Consumer Key and Secret: Credentials associated with the client application. The Consumer Key and Secret are used to generate an Access Token, which is needed to make calls to APIs that the application is subscribed to.
CustomerId: The Plexus Customer Number.
TenantId: It represents the customer on Plex IAM side. 
cURL: A command-line tool for transferring data in various protocols, in our case HTTP. Many of the API example calls are made in the cURL syntax.
JSON (JavaScript Object Notation): Much like XML, it is a way to transport and store data. JSON is smaller than XML and easier to parse.
UUID V4 (Universally Unique Identifier): A 128-bit number used to identify information in computer systems.
REST (REpresentational State Transfer): A stateless architecture that generally runs over HTTP. The REST style is based on resources (nouns) that have their own unique URI, and operations on those resources are limited to HTTP verbs (GET, POST, PUT, and DELETE). For example, consider a resource called Account. You would do an HTTP GET to retrieve information about that Account, and an HTTP POST to update the Account. REST-style APIs are easier to understand and consume, especially for mobile development.
OAuth (Open Authorization): An open standard for token-based authentication and authorization on the web. There are two types of OAuth: 2-legged, which authenticates the client application, and 3-legged, which authenticates the client application and the end-user. Currently, all of our APIs support 2-legged Oauth. 3-legged support is coming soon.
SOAP (Simple Object Access Protocol): This method of web services transmits messages via XML, usually as an HTTP POST. SOAP services contain service operations that are requested by the calling application. These service operations usually contain verb/noun combinations (such as getAccountByID, getAccountByName, UpdateAccount…). One advantage to SOAP is that it provides WSDL (Web Service Definition Language) that describes the service operations. Most large enterprise applications support SOAP.
SoapUI: A free and open-source graphical web service testing tool. It supports both SOAP and REST calls. Many of our API examples are documented in SoapUI.
Throttling: This limits the number of requests an application can make to an API. Most of the APIs found in the API directory are limited to 200 calls per minute.
XML (Extensible Markup Language): A way to transport and store data along with tags that say what the data is.
Properties: Name-value pairs that can be used in the API functionality. API developers can define different values for the same property in different environments such as sandbox, test, and production. The actual value is used at runtime in the environment where the client is consuming the API. For example, endpoint URL is a property that can define the actual API backend URL that is used in each environment at runtime.
Paths: Resource URLs that link to an API. Each path can have a GET/PUT/POST/DELETE HTTP action defined. Paths can have defined parameters. The parameters can be specified as required/optional and as Query/Path/header parameters. API developers can also specify the default values of the parameters.
Definitions: The syntax for any properties used in the API development. API developers can define a property’s behavior in this section, such as defining it as an Array/Object with specific fields, for example.
Tags: Metadata information about the API that is helpful when the API is published. Tags can be used by the consumers to search for the API. If the consumer searches by a tag that is defined in the API, the API appears in the search results. Multiple tags can be defined in an API.