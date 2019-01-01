# SPARK-R-SQL-NYC-PArking-Ticket-Analysis
New York City is a thriving metropolis. Just like most other metros that size, one of the biggest problems its citizens face is parking. The classic combination of a huge number of cars and a cramped geography is the exact recipe that leads to a huge number of parking tickets.

In an attempt to scientifically analyse this phenomenon, the NYC Police Department has collected data for parking tickets. Out of these, the data files from 2014 to 2017 are publicly available on Kaggle. We will try and perform some exploratory analysis on this data. Spark will allow us to analyse the full files at high speeds, as opposed to taking a series of random samples that will approximate the population.

Examine the data

Find the total number of tickets for each year.
Find out the number of unique states from where the cars that got parking tickets came from. (Hint: Use the column 'Registration State')
There is a numeric entry in the column which should be corrected. Replace it with the state having maximum entries. Give the number of unique states for each year again.
Some parking tickets don’t have the address for violation location on them, which is a cause for concern. Write a query to check the number of such tickets.
The values should not be deleted or imputed here. This is just a check.
 

Aggregation tasks

How often does each violation code occur? Display the frequency of the top five violation codes.
How often does each 'vehicle body type' get a parking ticket? How about the 'vehicle make'? (Hint: find the top 5 for both)
A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequency of tickets for each of the following:
'Violation Precinct' (this is the precinct of the zone where the violation occurred). Using this, can you make any insights for parking violations in any specific areas of the city?
'Issuer Precinct' (this is the precinct that issued the ticket)
Here you would have noticed that the dataframe has 'Violating Precinct' or 'Issuing Precinct' as '0'. These are the erroneous entries. Hence, provide the record for five correct precincts. (Hint: print top six entries after sorting)
Find the violation code frequency across three precincts which have issued the most number of tickets - do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts? 
Hint: You can analyse the three precincts together using the 'union all' attribute in SQL view. In the SQL view, use the 'where' attribute to filter among three precincts and combine them using 'union all'.
You’d want to find out the properties of parking violations across different times of the day:
Find a way to deal with missing values, if any.
Hint: Check for the null values using 'isNull' under the SQL. Also, to remove the null values, check the 'dropna' command in the API documentation.

The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.

Divide 24 hours into six equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups, find the three most commonly occurring violations.
Hint: Use the CASE-WHEN in SQL view to segregate into bins. For finding the most commonly occurring violations, a similar approach can be used as mention in the hint for question 4.

Now, try another direction. For the 3 most commonly occurring violation codes, find the most common time of the day (in terms of the bins from the previous part)

Let’s try and find some seasonality in this data

First, divide the year into some number of seasons, and find frequencies of tickets for each season. (Hint: Use Issue Date to segregate into seasons)

Then, find the three most common violations for each of these seasons.
(Hint: A similar approach can be used as mention in the hint for question 4.)

The fines collected from all the parking violation constitute a revenue source for the NYC police department. Let’s take an example of estimating that for the three most commonly occurring codes.
Find total occurrences of the three most common violation codes
Then, visit the website:
http://www1.nyc.gov/site/finance/vehicles/services-violation-codes.page
It lists the fines associated with different violation codes. They’re divided into two categories, one for the highest-density locations of the city, the other for the rest of the city. For simplicity, take an average of the two.
Using this information, find the total amount collected for the three violation codes with maximum tickets. State the code which has the highest total collection.
What can you intuitively infer from these findings?
