# Problem 2: ArXiv Paper Discovery with DynamoDB

## 1. Schema Design Decisions

### Why did you choose your partition key structure?

I chose the partition key structure based on the main way users search for papers, which is by category. The partition key stores the category name with a CATEGORY prefix so that all papers in the same field are grouped together in DynamoDB. The sort key contains the publication timestamp followed by the arXiv ID. This allows the papers inside a category to stay ordered by time. Because of this design it becomes easy to retrieve the most recent papers or filter papers within a specific date range using a normal DynamoDB query.

### How many GSIs did you create and why?

I created three global secondary indexes. The first index allows queries by author so that a user can find all papers written by a specific researcher. The second index allows direct lookup using the arXiv ID without needing to know the category. The third index supports keyword search using words that are extracted from the abstract. These indexes were required because DynamoDB cannot efficiently filter on attributes that are not part of a key. Without these GSIs many queries would require scanning the entire table.

### What denormalization trade offs did you make?

The main trade off in this schema is denormalization. Each paper is stored multiple times in order to support different query patterns. A single paper may appear under several categories, several authors, and several keywords. This increases storage usage and also increases the number of write operations when loading the data. However it makes the read operations very fast because each query can be answered using key based lookups instead of table scans.


## 2. Denormalization Analysis

### Average number of DynamoDB items per paper

In my dataset ten papers were loaded and a total of one hundred eighty five DynamoDB items were created. This results in an average of about eighteen and a half items per paper.

### Storage multiplication factor

Because each paper is stored multiple times for different access patterns, the data is multiplied by about eighteen and a half times compared to storing each paper only once.

### Which access patterns caused the most duplication?

The keyword search pattern caused the most duplication. Each paper generates up to ten keyword items based on the most frequent words in the abstract. Author queries also create duplication because many papers have multiple authors. Category items create fewer duplicates because most papers only belong to one or two categories.


## 3. Query Limitations

### What queries are NOT efficiently supported by your schema?

Some queries are not efficient with this schema. For example counting the total number of papers written by an author across the entire dataset would require reading all matching author items. Another example would be finding the most cited papers across all categories.

### Why are these difficult in DynamoDB?

These queries are difficult because DynamoDB is designed for key based lookups rather than global aggregations. DynamoDB works best when the access patterns are known ahead of time. Operations such as joins, global ranking, and large aggregations are not what DynamoDB is optimized for and would require scanning large portions of the table.


## 4. When to Use DynamoDB

### Based on this exercise, when would you choose DynamoDB over PostgreSQL?

DynamoDB is a good choice when the application needs very fast lookups and the access patterns are already known. It works well for systems that need to scale to large amounts of traffic and where the queries are predictable.

### What are the key trade offs?

The main trade off is between performance and flexibility. DynamoDB provides very fast performance for predefined queries but requires denormalization and careful schema design. PostgreSQL is more flexible and supports joins, aggregations, and complex queries, but it may require more effort to scale for very large workloads.