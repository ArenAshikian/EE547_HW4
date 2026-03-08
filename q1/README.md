Q1
1. Schema Decisions: Natural vs surrogate keys? Why?

I stuck with natural keys here because the dataset was already giving me solid, unique identifiers like line_name and trip_id. Honestly, adding an artificial Auto-Increment ID column felt like extra baggage when the real-world data already did the job. For the mapping tables (like line_stops), I went with composite keys. It just makes more sense—a record is defined by the specific combination of the line and the stop, so why not let the primary key reflect that reality?



2. Constraints: What CHECK/UNIQUE constraints did you add?

I didn't want the database to turn into a "garbage in, garbage out" situation, so I leaned heavily on CHECK and UNIQUE constraints. Beyond the standard Primary and Foreign keys, I made sure things like passenger counts or time offsets can’t be negative—because you can’t exactly have -5 people boarding a bus. I also threw in uniqueness constraints on our bridge tables to make sure we don't accidentally double-map a stop to the same route.



3. Complex Query: Which query was hardest? Why?

The toughest part was definitely pulling the stops with above-average ridership. You can’t just ask SQL for "groups where the sum is greater than the average" in one go because of how the execution order works. I had to calculate the total boardings per stop first, then run a subquery to find the global average, and finally join them up to filter the results. Getting those layers of aggregation to play nice together took a few tries.



4. Foreign Keys: Give example of invalid data they prevent

Foreign keys are basically my safety net. For example, in the stop_events table, every event has to link back to a valid trip_id. Without that FK constraint, you could accidentally log a passenger boarding a trip that doesn't even exist in the system. It keeps the data from becoming a "ghost town" of orphaned records that don't lead anywhere.



5. When Relational: Why is SQL good for this domain?

Transit data is almost built for SQL. Everything is a relationship: a route has many stops, a trip follows a route, and an event happens at a stop. It’s all very "grid-like." Using a relational DB allows us to enforce those rules strictly. Plus, when you need to do something like "find the busiest stop on the Blue Line," the ability to JOIN tables and aggregate data is way more efficient than trying to parse through a bunch of loose JSON files.