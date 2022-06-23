# sql_practice
A place for practice queries to live forever :)


### QUERY 1

* Here I have joined three tables: orders (1.5 billion rows), nation (25 rows), customer (150 million rows). The question I asked here was, how is how much money is tied into the order status of an item across all countries. 


![query_1_heatmap](/resources/query_1_heatmap.png)

<br></br>

* There isn't much variation across the order status groups based on the heat map. I conducted further analysis to find the standard deviation between each of the status groups.
    * The heat map wasn't granular enough to reflect that in a sum of almost five trillion dollars, that the stddev was 2 billion for groups F and O. Small compared to the trillions, but also a small fortune. 
    * With that being said it's all still sample data. 

![query_1_heatmap](/resources/query_1_stddev.png)


