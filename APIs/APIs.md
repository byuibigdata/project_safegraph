# Leveraging APIs for data sharing

## JavaScript Object Notation (JSON Format)

> Today, JSON is the de-facto standard for exchanging data between web and mobile clients and back-end services. [ref](https://www.infoworld.com/article/3222851/what-is-json-a-better-format-for-data-exchange.html)

> Well, when you’re writing frontend code in Javascript, getting JSON data back makes it easier to load that data into an object tree and work with it. And JSON formats data in a more succinct way, which saves bandwidth and improves response times when sending messages back and forth to a server.
>   
>In a world of APIs, cloud computing, and ever-growing data, JSON has a big role to play in greasing the wheels of a modern, open web. [ref](https://blog.sqlizer.io/posts/json-history/)

## REST APIs

> Over the course of the ’00s, another Web services technology, called Representational State Transfer, or REST, began to overtake [all other tools] for the purpose of transferring data. One of the big advantages of programming using REST APIs is that you can use multiple data formats — not just XML, but JSON and HTML as well. As web developers came to prefer JSON over XML, so too did they come to favor REST over SOAP. As Kostyantyn Kharchenko put it on the Svitla blog, “In many ways, the success of REST is due to the JSON format because of its easy use on various platforms.” [ref](https://www.infoworld.com/article/3222851/what-is-json-a-better-format-for-data-exchange.html)

## GraphQL APIs

> GraphQL on the other hand is a query language which gives the client the power to request specific fields and elements it wants to retrieve from the server. It is, loosely speaking, __some kind of SQL for the Web__. It therefore has to have knowledge on the available data beforehand which couples clients somehow to the server. ([ref](https://stackoverflow.com/questions/48022349/what-is-difference-between-rest-api-and-graph-api) and [another reference](https://zapier.com/engineering/graph-apis/)

## API Documentation

## Using keys

See the `create_environ.py` file for an example.

## Parsing JSON 

JSON is simply a flat text file that follows a specific format.  Python handles JSON files leveraging [lists](https://www.programiz.com/python-programming/methods/list/append) and [dictionaries](https://www.programiz.com/python-programming/methods/dictionary) to represent the details in a JSON data object.

### Using Pandas `json_normalize()`

> Normalize semi-structured JSON data into a flat table. [reference](https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html)

### Using Pyspark with jsonlines (`.jsonl`)

> This data format is straight-forward: it is simply one valid JSON value per line, encoded using UTF-8. While code to consume and create such data is not that complex, it quickly becomes non-trivial enough to warrant a dedicated library when adding data validation, error handling, support for both binary and text streams, and so on. This small library implements all that (and more!) so that applications using this format do not have to reinvent the wheel. [jsonlines](https://jsonlines.readthedocs.io/en/latest/)

[SparkByExamples.com](https://sparkbyexamples.com/) provides some [details on how Spark interacts with JSON data files](https://sparkbyexamples.com/spark/spark-read-and-write-json-file/).

## Thought questions

_After using the API to retrieve a few SafeGraph observations, let's parse the data into a table._

1. Use the `graphql_noauth.py` and extend the current `query()` to include `image`, `origin`, and `episode`.
2. Convert the episode field from key/value (wide format) to a JSON array (long format) with the keys `number`, `name` and `air_date` and their associated arrays `[]` as the values.
3. Use `pd.parse_json()` to build your table. What do you notice?
4. Save your new `.jsonl` output to the `jsonlines` format using the [jsonlines python package](https://jsonlines.readthedocs.io/en/latest/). Your file should look like [output.jsonl](output.jsonl)
5. Read the `.jsonl` file with `pd.read_json()` and the correct arguments.

### `.jsonl` and array structure

Notice that the API returns the key/value pair for `episode` as

```JSON
'episode': [{'episode': 'S01E10',
   'name': 'Close Rick-counters of the Rick Kind',
   'air_date': 'April 7, 2014'},
  {'episode': 'S03E07',
   'name': 'The Ricklantis Mixup',
   'air_date': 'September 10, 2017'}]}
```

We will also want to save the file as a [`.jsonl`](https://jsonlines.org/) with one record (valid JSON value) per line.