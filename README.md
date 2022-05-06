# compare_DB_connections
To start the program, run the command
```
python main.py <operation>
```
, where *\<operation\>* can be:
- *create*
- *read*
- *update*
- *delete*

Before starting, you need to set the parameters in connection.py:
- LOCAL_HOST 
- LOCAL_PORT
- DOCKER_HOST
- DOCKER_PORT
- CLOUD_CONNECTION
# Create
As a result of the program execution, the following image will be displayed.
![Create](https://github.com/MVjimboUniversity/compare_DB_connections/blob/main/create.png?raw=true "Create")
- Left subplot shows results results of data insertion using *insert_one* 
for one document at a time.
- Middle subplot shows results of data insertion using *insert_many*
for several documents at a time.
- Right subplot shows results of data insertion using *bulk_write(insert_one)*
for several insert requests at a time.

As we can see, inserting data using *insert_one* takes a lot of time. Both *insert_many*
and *bulk_write(insert_one)* takes the same time.

Also inserting data with cloud connection takes much more time then the others.
So it is better to use *insert_many* and *bulk_write(insert_one)*
with cloud connection.
# Read
![Read](https://github.com/MVjimboUniversity/compare_DB_connections/blob/main/read.png?raw=true "Read")
- Left subplot shows result for querying all objects from collection. 
As we can see the time does not depend on the size of collection.
- Right subplot shows result for querying using range operator.
As we can see the time does not depend on the size of collection.

Also time doesn't depend on connection type.
# Update
![Update](https://github.com/MVjimboUniversity/compare_DB_connections/blob/main/update.png?raw=true "Update")
- Left subplot shows result for updating every object in collection one by one using *update_one*.
- Middle subplot shows result for updating several objects at a time using *update_many*.
- Right subplot shows result for updating several objects at a time using *bulk_write(update_one)*.

We can see the same results as in the operation **Create**.
Only the operation *bulk_write(update_one)* takes 2 times longer than operation *update_many*.
#Delete
![Delete](https://github.com/MVjimboUniversity/compare_DB_connections/blob/main/delete.png?raw=true "Delete")
- Left subplot shows result for deleting every object in collection one by one using *delete_one*.
- Middle subplot shows result for deleting several objects at a time using *delete_many*.
- Right subplot shows result for deleting several objects at a time using *bulk_write(delete_one)*.
We can see the same results as in the operation **Update**.