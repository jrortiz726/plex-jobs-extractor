## Sample Data Source API requests and responses
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