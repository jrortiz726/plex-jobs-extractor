## Cognite Data Fusion - Mappings
Custom message formats
Use the set of predefined message formats or define your own message formats as a mapping from the source data format to target data types in CDF.

A format can produce multiple different types of resources. The output must be a list of JSON objects or a single JSON object, where each matches an output resource schema described below.

Note that the output can contain multiple different types, for example:

[
  {
    "type": "datapoint",
    "externalId": "my-ts",
    "timestamp": 1710842060124,
    "value": 123
  },
  {
    "type": "datapoint",
    "externalId": "my-ts",
    "timestamp": 1710842061124,
    "value": 124
  },
  {
    "type": "time_series",
    "externalId": "my-ts",
    "name": "My Time Series",
    "isString": false
  }
]

Data points
Each output is a single CDF data point mapped to a CDF time series. If the time series doesn't exist, it will be created.

type: Required, set to datapoint.
externalId: The external ID of the time series to insert the data point into.
space: The name of the data modeling space to write the data points to.
timestamp: The timestamp of the data point is given as a millisecond Unix timestamp.
value: The data point value as a number or a string. This may also be null, but only if status is specified and a variant of Bad.
status: Data point status code symbol as defined in the time series docs.
isStep: The value of isStep used when creating the time series for this datapoint if it does not already exist.
Events
Each output is a single CDF event. The old event will be updated if the provided event already exists and has an external ID. Null fields will be ignored when updating events. This is almost identical to the "Create event" payload to the Cognite API. See the API docs on events for details.

type: Required, set to event.
externalId: Required, the external ID of the event.
startTime: The start time of the event, given as a millisecond Unix timestamp.
endTime: The end time of the event, given as a millisecond Unix timestamp.
eventType: The type field of the event in CDF.
subtype: The subtype field of the event in CDF.
description: The description field of the event in CDF.
metadata: An object containing key/value pairs for event metadata. Both keys and values must be strings.
assetIds: A list of asset external IDs to link the event to. If the assets do not exist, they will not be created.
source: The source field of the event in CDF.
CDF RAW
Each output is a single raw row, which will be ingested directly into CDF RAW. The extractor may skip ingesting raw rows that haven't changed since they were last seen.

type: Required, set to raw_row.
table: The RAW table to write to.
database: The RAW database to write to.
key: The key of the row to be written to RAW.
columns: A JSON object containing the columns to be written to RAW.
Time series
Each output is a single time series. If the provided time series external ID already exists, then the old time series will be updated. Null fields will be ignored when updating events. This is almost identical to the "Create time series" payload to the Cognite API. See the API docs on time series for details.

type: Required, set to time_series.
externalId: Required, the external ID of the time series.
name: The name of the time series.
isString: Defines whether the time series contains string values (true) or numeric values (false). This value cannot be updated. Defaults to false.
metadata: An object containing key-value pairs for time series metadata. Both keys and values must be strings.
unit: The physical unit of the time series as free text.
unitExternalId: The physical unit of the time series as represented in the unit catalog. Only available for numeric time series.
assetId: Asset external ID to link the time series. If the asset does not exist, it will not be created.
isStep: Whether the time series is a step series or not.
description: Free text description of the time series.
securityCategories: List of security categories required to access the created time series.
Data models
Each output is a node or edge, written to a single view. Values are overwritten. Your mapping can produce a list of nodes or edges. If multiple outputs have the same external ID and space, these are combined. This lets you simultaneously ingest data into multiple views. See the API docs for details.

Nodes
type: Required, set to node.
externalId: Required, the external ID of the node.
space: Required, the space of the node.
nodeType: A direct relation to a different node representing the type of this node.
externalId: Required, type external ID.
space: Required, type space.
view: Required, the view being written to.
externalId: Required, view external ID.
space: Required, view space.
version: Required, view version.
properties: Required, a (maybe empty) object containing the properties being written, see API docs for the string representation of data modeling data types.
Edges
type: Required, set to edge.
externalId: Required, the external ID of the node.
space: Required, the space of the node.
edgeType: Required, a direct relation to a node representing the type of this edge.
externalId: Required, type external ID.
space: Required, type space.
view: Required, the view being written to.
externalId: Required, view external ID.
space: Required, view space.
version: Required, view version.
properties: Required, a (maybe empty) object containing the properties being written, see API docs for the string representation of data modeling data types.
startNode: Required, the start node of the edge.
externalId: Required, start node external ID.
space: Required, start node space.
endNode: Required, the end node of the edge.
externalId: Required, end node external ID.
space: Required, end node space.
Example
This section explains how you can set up your own message formats for hosted extractors in CDF using the MQTT extractor as an example.

The language you must use to transform the data is inspired by JavaScript. If you want a complete overview and descriptions of all the available functions, see All built-in functions.

tip
For more details about the mapping language, see Mapping concepts.

Single data point in a message
The section below describes how to set up message formats using a broker where the data comes in as values, and the topic indicates which sensor generated the value. For example, if you have a sensor that publishes the message

23.5

on the topic /myhouse/groundfloor/livingroom/temperature and you want to map this to a target in CDF. In this example, the data is ingested as a data point in a time series, where the content is the value, the time the message was received as the timestamp, and the topic path as the external ID of the time series.

These are the input objects for defining the format:

input, which is the content of the message received
context, which contains information about the message, such as which topic it arrived on
To make a data point from this message, create a JSON object with four fields:

value: The value of the data point.
timestamp: The timestamp for the data point given as a millisecond Unix timestamp. Use the built-in now() function to get the current time on this format.
externalId: The external ID of the time series to insert this data point into.
type: Set to datapoint to tell the extractor that this JSON object describes a data point.
The final transform looks like:

{
    "value": input,
    "timestamp": now(),
    "externalId": context.topic,
    "type": "datapoint"
}

Handling more data in a single message
This section describes how to set up message formats while subscribing to a topic that contains the external ID of the time series getting the data, but the message payloads are now lists of data points instead of just a single value:

{
  "sensorData": [
    {
      "datetime": "2023-06-13T14:52:34",
      "temperature": 21.4
    },
    {
      "datetime": "2023-06-13T14:59:53",
      "temperature": 22.1
    },
    {
      "datetime": "2023-06-13T15:23:42",
      "temperature": 24.0
    }
  ]
}

The content of messages will be automatically parsed as JSON and made available through the input object. In the last example, you used the input object directly, as the messages had no structure. This time, you can access attributes the same way you might be familiar with from object-oriented languages. For example input.sensorData[0].temperature will resolve to 21.4.

To ingest this data into CDF, you must make a data point for each element in the sensorData list. Use a map function on input.sensorData. map takes in a function and applies that function to each element in the list.

input.sensorData.map(row =>
    ...
)

row is a name for the input of the map function. In this case, for the first iteration of the map the row object will look like

{
  "datetime": "2023-06-13T14:52:34",
  "temperature": 21.4
}

The output of the map function should be

{
  "value": 21.4,
  "timestamp": 1686667954000,
  "externalId": "/myhouse/groundfloor/livingroom/temperature",
  "type": "datapoint"
}

To do this, you must define a JSON structure where you specify input data as the values:

{
    "value": row.temperature,
    "timestamp": to_unix_timestamp(row.datetime, "%Y-%m-%dT%H:%M:%S"),
    "externalId": context.topic,
    "type": "datapoint"
}

For value, we map it to the temperature attribute of the row object. Similarly, for timestamp, except that we need to parse the time format from a string to a CDF timestamp. To do this, we use the to_unix_timestamp function, which takes in the timestamp to convert, and a description of the format.

For the external ID of the time series to use, we do the same as in the previous example and use the topic the message arrived at. And type can just be hard coded to datapoint since we only ingest data points in this example.

Putting that all together, we end up with the following format description:

input.sensorData.map(row => {
    "value": row.temperature,
    "timestamp": to_unix_timestamp(row.datetime, "%Y-%m-%dT%H:%M:%S"),
    "externalId": context.topic,
    "type": "datapoint"
})

Nested structures
Finally, let's look at a case where the data is nested with several lists. For example, let's consider the case where a message contains a list of time series, each with a list of data points:

{
  "sensorData": [
    {
      "sensor": "temperature",
      "location": "myhouse/groundfloor/livingroom",
      "values": [
        {
          "datetime": "2023-06-13T14:52:34",
          "value": 21.4
        },
        {
          "datetime": "2023-06-13T14:59:53",
          "value": 22.1
        },
        {
          "datetime": "2023-06-13T15:23:42",
          "value": 24.0
        }
      ]
    },
    {
      "sensor": "pressure",
      "location": "myhouse/groundfloor/livingroom",
      "values": [
        {
          "datetime": "2023-06-13T14:52:34",
          "value": 997.3
        },
        {
          "datetime": "2023-06-13T14:59:53",
          "value": 995.1
        },
        {
          "datetime": "2023-06-13T15:23:42",
          "value": 1012.8
        }
      ]
    }
  ]
}

First, let's start by iterating over the sensorData list in the same way as before:

input.sensorData.map(timeseries =>
    ...
)

For the first iteration in this map, the timeseries object will then be

{
  "sensor": "temperature",
  "location": "myhouse/groundfloor/livingroom",
  "values": [
    {
      "datetime": "2023-06-13T14:52:34",
      "value": 21.4
    },
    {
      "datetime": "2023-06-13T14:59:53",
      "value": 22.1
    },
    {
      "datetime": "2023-06-13T15:23:42",
      "value": 24.0
    }
  ]
}

To extract the data points from this object, we need to iterate over the values list. Let's attempt to use map again to do that:

input.sensorData.map(timeseries =>
    timeseries.values.map(datapoint =>
        ...
    )
)

For the first iteration of this inner map, the datapoint object will be

{
  "datetime": "2023-06-13T14:52:34",
  "value": 21.4
}

We can convert this to a data point JSON in a similar way to before:

{
    "value": datapoint.value,
    "timestamp": to_unix_timestamp(datapoint.datetime, "%Y-%m-%dT%H:%M:%S"),
    "externalId": concat(timeseries.location, "/", timeseries.sensor),
    "type": "datapoint"
}

We also need to make an external ID for the time series ourselves. To do this, we use the location and sensor attributes on the timeseries object from the outer loop and join them together with the concat function. Notice that in this inner loop, both the timeseries object from the outer map and the datapoint object from the inner map are available.

Putting this all together, we get

input.sensorData.map(timeseries =>
    timeseries.values.map(datapoint => {
        "value": datapoint.value,
        "timestamp": to_unix_timestamp(datapoint.datetime, "%Y-%m-%dT%H:%M:%S"),
        "externalId": concat(timeseries.location, "/", timeseries.sensor),
        "type": "datapoint"
    })
)

However, if we use this format to convert the example message, we will not get a list of data points, but a list of lists of data points:

[
  [
    {
      "externalId": "myhouse/groundfloor/livingroom/temperature",
      "timestamp": 1686667954000,
      "type": "datapoint",
      "value": 21.4
    },
    {
      "externalId": "myhouse/groundfloor/livingroom/temperature",
      "timestamp": 1686668393000,
      "type": "datapoint",
      "value": 22.1
    },
    {
      "externalId": "myhouse/groundfloor/livingroom/temperature",
      "timestamp": 1686669822000,
      "type": "datapoint",
      "value": 24
    }
  ],
  [
    {
      "externalId": "myhouse/groundfloor/livingroom/pressure",
      "timestamp": 1686667954000,
      "type": "datapoint",
      "value": 997.3
    },
    {
      "externalId": "myhouse/groundfloor/livingroom/pressure",
      "timestamp": 1686668393000,
      "type": "datapoint",
      "value": 995.1
    },
    {
      "externalId": "myhouse/groundfloor/livingroom/pressure",
      "timestamp": 1686669822000,
      "type": "datapoint",
      "value": 1012.8
    }
  ]
]

This is because map always works on a list and returns a new list. Since we want our output to be a list of data points, we need to change the outter map to a flatmap. flatmap is similar to map, except it flattens the output, which means that it rolls out the list of lists to just a simple list:

[
  {
    "externalId": "myhouse/groundfloor/livingroom/temperature",
    "timestamp": 1686667954000,
    "type": "datapoint",
    "value": 21.4
  },
  {
    "externalId": "myhouse/groundfloor/livingroom/temperature",
    "timestamp": 1686668393000,
    "type": "datapoint",
    "value": 22.1
  },
  {
    "externalId": "myhouse/groundfloor/livingroom/temperature",
    "timestamp": 1686669822000,
    "type": "datapoint",
    "value": 24
  },
  {
    "externalId": "myhouse/groundfloor/livingroom/pressure",
    "timestamp": 1686667954000,
    "type": "datapoint",
    "value": 997.3
  },
  {
    "externalId": "myhouse/groundfloor/livingroom/pressure",
    "timestamp": 1686668393000,
    "type": "datapoint",
    "value": 995.1
  },
  {
    "externalId": "myhouse/groundfloor/livingroom/pressure",
    "timestamp": 1686669822000,
    "type": "datapoint",
    "value": 1012.8
  }
]

In total, our final format looks like

input.sensorData.flatmap(timeseries =>
    timeseries.values.map(datapoint => {
        "value": datapoint.value,
        "timestamp": to_unix_timestamp(datapoint.datetime, "%Y-%m-%dT%H:%M:%S"),
        "externalId": concat(timeseries.location, "/", timeseries.sensor),
        "type": "datapoint"
    })
)

tip
For more details about the mapping language, see Mapping concepts.

Cookbook
This section contains examples of common patterns in payloads, and mappings to handle them.

Single data point with ID
Each message is a single data point with ID:

{
  "tag": "my-tag",
  "value": 123,
  "timestamp": "2023-06-13T14:52:34"
}

This should be mapped to a single data point in CDF:

{
  "type": "datapoint",
  "timestamp": to_unix_timestamp(input.timestamp, "%Y-%m-%dT%H:%M:%S"),
  "value": input.value,
  "externalId": input.tag
}

Sometimes, the value may be null, or some other value not accepted by the Cognite API. Mappings can return empty arrays, in which case nothing will be written to CDF:

[{
  "type": "datapoint",
  "timestamp": to_unix_timestamp(input.timestamp, "%Y-%m-%dT%H:%M:%S"),
  "value": try_float(input.value, null),
  "externalId": input.tag
}].filter(datapoint => datapoint.value is not null)

Multiple data points, single timestamp
A relatively common pattern is that a data point contains several measurements with a single timestamp.

{
  "sensorId": "my-sensor",
  "timestamp": "2023-06-13T14:52:34",
  "humidity": 123.456,
  "pressure": 321.654,
  "temperature": 15.1
}

Select these dynamically:

["humidity", "pressure", "temperature"]
  .map(field => {
    "type": "datapoint",
    "timestamp": to_unix_timestamp(input.timestamp, "%Y-%m-%dT%H:%M:%S"),
    "value": try_float(input[field], null),
    "externalId": concat(input.sensorId, "/", field)
  })
  .filter(datapoint => datapoint.value is not null)

If the actual fields are not known, you can even pick them dynamically using pairs:

input.pairs()
  .filter(pair => pair.key != "sensorId" && pair.key != "timestamp")
  .map(pair => {
    "type": "datapoint",
    "timestamp": to_unix_timestamp(input.timestamp, "%Y-%m-%dT%H:%M:%S"),
    "value": try_float(pair.value, null),
    "externalId": concat(input.sensorId, "/", pair.key)
  })
  .filter(datapoint => datapoint.value is not null)

Sample data into a raw table
Avoid doing this for very large data volumes. When developing a connection, it can sometimes be nice to get a sample of the data being ingested. You can write it to CDF Raw using a simple mapping like:

{
  "type": "raw_row",
  "table": "sample-table",
  "database": "sample-db",
  "key": string(now()),
  "columns": input
}