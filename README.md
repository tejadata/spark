# spark-streaming

In this you will find how to read/write structured data from file/socket using stuctured streaming API for more information visit www.waytodatascience.com
  1) appendMode.scala : In this we are reading data from a file as stream and appending it in the console for write stream
  2) completeMode.scala: In this we are reading text data from socket, doing word count with groupby/aggregate and writing the result in to the console using append mode
  3) timeStamp.scala : we are appending the time stamp to our stream/event data that will help for window funciton, We used UDF for Time stamp for adding new column on streaming Dataframe as the defualt function current_timestamp() will only work for Bulk processing 
