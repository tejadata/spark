# NOTE

From spark 2.x we have unified API for both bulk and stream data processing, we can use all api which is avilable for Bulk in streaming, the major difference we see is for bulk we use read/write option where for streaming we use readStream/writeStream

# spark-streaming

In this you will find how to read/write structured data from file/socket using stuctured streaming API for more information visit www.waytodatascience.com
  1) appendMode.scala : In this we are reading data from a file as stream and appending it in the console for write stream
  2) completeMode.scala: In this we are reading text data from socket, doing word count with groupby/aggregate and writing the result in to the console using append mode
  3) timeStamp.scala : we are appending the time stamp to our stream/event data that will help for window funciton, We used UDF for Time stamp for adding new column on streaming Dataframe as the defualt function current_timestamp() will only work for Bulk processing 
  4) windowFunction.scala : only change in this file when compare to timeStamp.scala is we used windows function on groupBy to group the price range in every five seconds (In real time lets a customer wants to know how many orders he processed/shipped/deliverd/failed in perticular time period on the real time data we will use window function)
 
 
 # Bulk processing
In this we will see how to process Bulk data

1) Dealing_with_Null_values.ipynb:  In this we will see how to del with missing values like filling/dropping etc...
2) ComplexTypeWithJSON.ipynb: In this we will see how to read mutiple lines JSON file with mutiple occurence  and split it in to two different dataframes 
3) RetailandPromotion_analysys.ipynb: In this we will see how to use basic groupby and orderby scenarios using Retail data
4) PIVOT_function.ipynb: In this you will learn how to use PIVOT function and it uses 

# Machine Learning and Statistics 
In this we will see how to use different 

1) Regression_analysis_On_USA_Housing_data.ipynb: In this we will discuss how to build Linear Regression model and save the model for further use, We will also see how to use Imputer to Impute the Missing values
2) Predicting_Heart_Disease_using_RandomForest.ipynb: In this we will see how to use Random Forest and Cross Validaiton using the data avilable from https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.cleveland.data
3) Imputation_Correlation_Outliers.ipynb: In this you learn how to use pearson Correlation, Filling missing value with Impute class from ML Lib, detecting outliers using IQR
