
-- KEY WORDS
-- OLTP (On-line Transaction Processing) is involved in the operation of a particular system. OLTP is characterized by a large number of short on-line transactions (INSERT, UPDATE, DELETE). The main emphasis for OLTP systems is put on very fast query processing, maintaining data integrity in multi-access environments and an effectiveness measured by number of transactions per second. In OLTP database there is detailed and current data, and schema used to store transactional databases is the entity model (usually 3NF). It involves Queries accessing individual record like Update your Email in Company database.
-- OLAP (On-line Analytical Processing) deals with Historical Data or Archival Data. OLAP is characterized by relatively low volume of transactions. Queries are often very complex and involve aggregations. For OLAP systems a response time is an effectiveness measure. OLAP applications are widely used by Data Mining techniques. In OLAP database there is aggregated, historical data, stored in multi-dimensional schemas (usually star schema). Sometime query need to access large amount of data in Management records like what was the profit of your company in last year.
-- Binomial distribution
-- Discrete Possibility distribution 
-- Example bell curve or S curve
-- Standard error of the mean measures how precise a sample mean is as an estimate of population mean. To calculate standard error of the mean, first we need to calculate the standard deviation, which is the spread of the sample, with the following equation:
-- Basically, the larger the larger the sample, the lower the standard error of the mean.
 
-- SNOWFLAKE  NOTES
-- Data size scalable ondemand. Makes latency issues much faster.
-- Using s3 bucket
-- A STAGE is essentially on top an S3 bucket. Analogous to a data lake for an S3 Bucket.
-- Create or replace stage citibike_trips url = ‘blah blah blah’ 

-- THOUGHT PROCESS 
-- READ THE QUESTION 3x
-- DOES THE CURRENT DATA HAVE THE ANSWERES, IF NOT WHAT MANIPULATIONS CAN WE USE
-- SET A STOP WATCH FOR 2-3 Minutes and just explore the data. 
-- Think about the output, and work your way backwards.
-- Will you need a sub query? A sub query is basically just a variable within a groupby. s
-- IF YOU NEED NUMBER OF USERS COUNT(DISTINCT USER)

-- UNIONS

-- JOIN Vs UNION
-- Join - with two different tables structures, puts the tables side by side where the join conditions are met
-- Union - with tables being exactly the same, creates one data set. 
-- Doesn’t require any condition for joining. UNION alone gets rid of duplicates. UNION ALL keeps duplicates.

-- INNER JOIN vs LEFT JOIN? Actually, that is not the question at all. 
-- You’ll use INNER JOIN when you want to return only records having pair on both sides, and you’ll use LEFT JOIN when you need all records from the “left” table,
-- no matter if they have pair in the “right” table or not. If you’ll need all records from both tables,
--  no matter if they have pair, you’ll need to use CROSS JOIN (or simulate it using LEFT JOINs and UNION).
--   More about that in the upcoming articles.
SELECT id, name FROM Turtles
UNION
SELECT id, name FROM Ninja_Turtles

-- EXAMPLE FOR MAKING A TABLE SUMETRIC 
SELECT 
  user_id, 
  friend_id
FROM 
  google_friends_network
UNION
SELECT 
  friend_id as user_id, -- notice the flip.
  user_id as friend_id
FROM google_friends_network
-- SQL FIZZ BUZZ 

with recursive numbers (i) as (
   select *
   from (values (1)) as t
   union all
   
   select n.i + 1
   from numbers n
   where n.i < 100
)
select i,
       case 
          when mod(i, 15) = 0 then 'Fizz Buzz'
          when mod(i,  5) = 0 then 'Buzz'
          when mod(i,  3) = 0 then 'Fizz'
          else cast(i as varchar)
       end as fz
from numbers 
order by i;

-- RATIO EXAMPLE
-- can use this case for like a price when sold or something. 
SELECT nominee, SUM(CASE WHEN winner = 'true' THEN 1.0 ELSE 0.0 END) / COUNT(*) as ratio
FROM oscar_nominees
GROUP BY 1
ORDER BY 2 DESC

-- PARITIONING SECTION
-- HOW TO RANK THINGS

SELECT DENSE_RANK() OVER(ORDER BY SUM(n_messages) DESC) as ranking,
       id_guest, SUM(n_messages)
FROM airbnb_contacts
GROUP BY id_guest
ORDER BY SUM(n_messages)
ORDER BY SUM(n_messages) DESC

-- remember that when you are ranking a count it needs to be in dsec
-- FIRST INSTANCE FIRST RECORD
SELECT company_id, user_id, call_rank
FROM (SELECT u.company_id, c.user_id , dense_rank() over(partition by u.company_id order by count(c.call_id) DESC) as call_rank
    FROM rc_calls c
    JOIN rc_users u ON c.user_id = u.user_id
    GROUP BY u.company_id,u.user_id, c.user_id) c
WHERE call_rank in (1,2)

-- BEST FIRST INSTANCE FIRST RECORD
-- PARTITION BY is more of a "specify by" than a "group by"
-- Go over this range ordered this way, but specify each user_id.
-- Then the distinct filters it, because all the user_id's have the same balance. 
select distinct(user_id), FIRST_VALUE(s.balance) OVER(PARTITION BY l.user_id ORDER BY created_at DESC) as   balance
      FROM loans l
         LEFT JOIN submissions s
              on l.id = s.loan_id
      WHERE type in ('Refinance')


-- ANSI SQL Partition by example. 
SELECT Customercity, 
       AVG(Orderamount) OVER(PARTITION BY Customercity) AS AvgOrderAmount, 
       MIN(OrderAmount) OVER(PARTITION BY Customercity) AS MinOrderAmount, 
       SUM(Orderamount) OVER(PARTITION BY Customercity) TotalOrderAmount
FROM [dbo].[Orders];


-- Partitioning sub query. max age per class titanic.
SELECT name, pclass,age
FROM (
SELECT name, pclass,age,dense_rank() OVER(PARTITION BY pclass ORDER BY age DESC) as age_rank
FROM titanic
WHERE survived = 1
      AND AGE IS NOT NULL) tmp
WHERE tmp.age_rank = 1

-- BUT HERE IS A WAY TO DO IT WITH OUT
SELECT t.pclass,
       t.name,
       t.age
FROM titanic t
INNER JOIN
  (SELECT pclass,
          MAX(age) AS oldest_survivor_age
   FROM titanic
   WHERE survived = 1
   GROUP BY pclass) tmp ON t.pclass = tmp.pclass
AND t.age = tmp.oldest_survivor_age -- a little hackey with the join to give us max age. 
WHERE survived=1
ORDER BY pclass

-- WINDOW FUNCTIONS

-- Worst 10 hotels
WITH distinct_hotels AS -- distinct_hotels needed because we need to rank them amongst all that other bull shit. 
  (SELECT DISTINCT hotel_name,
                   average_score
   FROM hotel_reviews),
     ranking_cte AS -- then we use ranking_cte to get the ranking
  (SELECT hotel_name, -- from distinct_hotels
          average_score,
          rank() OVER (
                       ORDER BY average_score ASC) AS rnk
   FROM distinct_hotels)
SELECT hotel_name,
       average_score
FROM ranking_cte
WHERE rnk <= 10

-- FIND SESSIONSWHERE FIRST SESSION WAS VIEWER

SELECT user_id, count(*) n_sessions
FROM twitch_sessions
WHERE session_type = 'streamer'
  AND user_id in
    (SELECT user_id
     FROM  -- this sub query is to make sure that we only get user_id
           -- because it would have too many columns for the where function otherwise. 
       (SELECT user_id,
               session_type,
               rank() OVER (PARTITION BY user_id
                            ORDER BY session_start) streams_order
        FROM twitch_sessions) s1
     WHERE streams_order =1
       AND session_type = 'viewer')
GROUP BY user_id
ORDER BY n_sessions DESC,
         user_id ASC


-- SUB QUERY IN WHERE CLAUSE
SELECT first_name,last_name, salary FROM employees
WHERE salary >
(SELECT max(salary) FROM employees
WHERE first_name='Alexander');


-- Day over Day analysis
SELECT inspection_date::DATE,
       COUNT(violation_id) - LAG(COUNT(violation_id)) OVER(
                                     ORDER BY inspection_date::DATE) diff
FROM sf_restaurant_health_violations
GROUP BY 1
ORDER BY 1

-- lag with out window function
select a1.id,a1.time
   a1.value as value, 
   b1.value as value_lag,
   c1.value as value_lead
into tab2
from tab1 a1
left join tab1 b1
on a1.id = b1.id
and a1.time-1= b1.time
left join tab1 c1
on a1.id = c1.id
and a1.time+1 = c1.time


-- month over month % analysis

SELECT to_char(created_at::date, 'YYYY-MM') AS year_month,
       round(((sum(value) - lag(sum(value), 1) OVER w) / (lag(sum(value), 1) OVER w)) * 100, 2) AS revenue_diff_pct
FROM sf_transactions
GROUP BY year_month 
WINDOW w AS (
                                 ORDER BY to_char(created_at::date, 'YYYY-MM'))
ORDER BY year_month ASC

SELECT 
    year_month,
    ROUND((value - LAG(value) OVER (ORDER BY year_month)) / LAG(value) OVER (ORDER BY year_month) * 100, 2) revenue_diff_pct 
FROM (
select to_char(created_at, 'YYYY-MM') as year_month, SUM(value) AS value 
from sf_transactions
GROUP BY 1
) qq


-- – use cte to be able to get the day itself. 
-- – use cte1 to be able to use dense_rank to partition over the oder_day

with cte as (
select *, order_timestamp::date as order_day
from doordash_orders as o
join doordash_merchants as m
ON o.merchant_id = m.id
), cte1 as (
select order_day, name, dense_rank() over(partition by order_day order by count(*) desc) as rnk
from cte
group by order_day, name)
select * from cte1 where rnk <4;


-- Most Recent 
-- https://www.postgresqltutorial.com/postgresql-window-function/postgresql-first_value-function/
WITH first_order AS
(SELECT customer_id,
FIRST_VALUE(merchant_id) OVER(PARTITION BY customer_id
ORDER BY order_timestamp) AS first_merchant
FROM doordash_orders),


-- EXCEPT FUNCTION AKA NOT IN or LOGICAL NOT

SELECT DISTINCT name
FROM olympics_athletes_events
WHERE team = 'Norway'
  AND sport = 'Alpine Skiing'
  AND YEAR = 1992
EXCEPT -- follows the same rule as the union
SELECT name
FROM olympics_athletes_events
WHERE YEAR = 1994

-- Counting if more than 1 null value. 
CASE FUNCTION 
SELECT *
FROM user_flags
WHERE (CASE WHEN user_firstname IS NULL 
       THEN 1 ELSE 0 END) + (CASE WHEN user_lastname IS NULL 
       THEN 1 ELSE 0 END) + (CASE WHEN video_id IS NULL 
       THEN 1 ELSE 0 END) + (CASE WHEN flag_id IS NULL 
       THEN 1 ELSE 0 END) > 1


-- DATE TIME SECTION

-- Getting the largest amount from a date range.
SELECT c.first_name, SUM(o.total_order_cost), o.order_date
FROM customers c
JOIN orders o
     ON c.id = o.cust_id
WHERE o.order_date BETWEEN '2019-02-01' AND '2019-05-01'
GROUP BY 1,3
ORDER BY 2 DESC
LIMIT 1x


WHERE EXTRACT(MONTH FROM order_date) = 3 AND
      EXTRACT(YEAR FROM order_date) = 2019


-- How to get exact month

EXTRACT(DAY FROM occurred_at - activated_at) 

-- In the select section to get the number of days

DATE_PART('year', CURRENT_DATE) - year <= 20
	Gives date range, and if its less than 20 years.

-- DIFFERENCE BETWEEN DATES
datediff('2020-02-10', created_at) BETWEEN 0 AND 30

-- DATE TIME CHEAT SHEET
-- https://kaiwern.com/posts/2020/04/14/essential-date-functions-for-grouping-in-postgresql/

SELECT date_part('dow', s.signup_start_date)

-- BIG NOTE you need a ::decimal for you to get a percentage!!!!!

-- churn rate across all eyars

SELECT COUNT(CASE WHEN end_date IS NOT NULL THEN 1 END) / COUNT(start_date) :: DECIMAL AS ratio
FROM lyft_drivers


-- https://www.bestinterviewquestion.com/postgresql-interview-questions

-- Technical Interview Notes

From this Article

SELECT: choose the columns
Commas after each EXCEPT for the last one. 
FROM: What Tables
JOIN: Merging tables. We specify only one table in the FROM clause
WHERE: Filtering the output
GROUP BY: Grouping by a specified condition.
HAVING: Also filters output, however at the GROUP level, not the individual level.
ORDER BY: Ordering the output. Default ascending.

Sarah’s
From 
Johanasburg
Where
Groups
Have 
Order 
Like and Not Like


-- STRINGS 
-- Like just means that it contains the string.
SELECT FirstName, LastName, Age
FROM person_info
WHERE LastName LIKE 'Peterson'


-- Can also include wild card ‘_’, so like ‘t_m’ can be tim, tom or tam

-- % is a wildcard for multiple characters. So ‘%son’ can be tomson, peterson, johnson.



-- REGEXing the year out of something
SELECT
    title,
    NULLIF(regexp_replace(title, '\D','','g'), ''):: NUMERIC AS year
FROM
    winemag_p2
WHERE 
    country = 'Macedonia'

-- You could do it like this regexp_replace(po_number, '\D','','g'):

-- \D means not a digit
-- second argument is replacement string, by replacing with empty string we're "removing" non-digit characters
-- g stands for global, meaning to do this for all occurrences of non digital characters

-- Difference between HAVING and WHERE here.

-- HAVING is for when you need conditions after a groupby



-- What does a JOIN do? What are the different types of JOIN clauses?

-- Inner Join, keeps only those that have matched.
-- Left Join keeps all from left table, and only the right ones that match
-- Full Join is just keeping all of them, but making sure the ones that can match do. 




-- Sub queries: 
-- Use as a condition for the outer query to specify the information we want retrieved. 



-- WHY USE A SUB QUERY? https://www.tutorialspoint.com/sql/sql-sub-queries.htm#:~:text=A%20subquery%20is%20used%20to,%2C%20IN%2C%20BETWEEN%2C%20etc.
   -- distinct user names for an agg functoin
   -- The difference between two points that are in one table. 
   -- heavier data manipulation, and the outer query is just popping it off the top. or averaging those heavier manipulations. 
-- good example of a sub query
SELECT (MAX(sum_score) - min(sum_score)) as score_difference
FROM(
SELECT student, SUM(assignment1 + assignment2 + assignment3) as sum_score
FROM box_scores
GROUP BY student) as t

-- WITH clause. 
-- Comes BEFORE FROM. Its essentially a way to make a sub query a variable. 
-- This variable is technically a TABLE.  For the rest of the query. 
-- Like if you need to set a variable within a data set that has multiple conditions to be met. 

ORDER OF THE COLUMNS IS IMPORTANT FOR SQL


-- MATH / FORMATTING
select count(p.promotion_id)/count(*)::float*100 as percentage
from facebook_sales s
left join facebook_promotions p
on p.promotion_id = s.promotion_id

-- best way to find the median
SELECT jobtitle, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY totalpay)
FROM sf_public_salaries
GROUP BY 1
ORDER BY 2 DESC



-- IF LOOP BUT IN SQL 
-- CASE IS BASICALLY A .loc
-- COUNT DISTINCT IS BIG HERE
SELECT users.language,
       COUNT (DISTINCT CASE
                           WHEN device IN ('macbook pro',
                                           'iphone 5s',
                                           'ipad air') THEN users.user_id
                           ELSE NULL
                       END) AS n_apple_users,
             COUNT(DISTINCT users.user_id) AS n_total_users
FROM playbook_users users
INNER JOIN playbook_events EVENTS ON users.user_id = events.user_id
GROUP BY users.language
ORDER BY n_total_users DESC



-- MATH 
-- STUPID GOOGLE QUESTION FOR SOME BULLSHIT I DONT KNOW

-- Formula to calculate distance with the curvature of the earth: 
--     - d = acos( sin φ1 ⋅ sin φ2 + cos φ1 ⋅ cos φ2 ⋅ cos Δλ ) ⋅ R
--     - R = 6371
--     - φ1 = lat1 * Math.PI/180
--     - φ2 = lat2 * Math.PI/180
-- Formula to calculate distance on a flat surface: 
--     - sqrt( (lat2-lat1)**2 + (lon2-lon1)**2) * D
--     - D = 111 (degree to km)

SELECT avg(distance_curvature) AS avg_distance_curvature,
       avg(distance_flat) AS avg_distance_flat,
       avg(distance_curvature) - avg(distance_flat) AS distance_difference
FROM -- first sub query
  (SELECT *,
          ACOS(SIN(RADIANS(latitude_1))*SIN(RADIANS(latitude_2)) + COS(radians(latitude_1))*COS(radians(latitude_2))*COS(radians(longitude_2 - longitude_1)))*6371 AS distance_curvature,
          sqrt((latitude_2-latitude_1)^2 +(longitude_2-longitude_1)^2)*111 AS distance_flat
   FROM --  2nd sub query
        -- so this is how we get a first location, and then a second location 
        -- From the same table. 
     (SELECT a1.user_id,
             a1.session_id,
             a1.day,
             a1.step_id,
             a1.latitude AS latitude_1,
             a1.longitude AS longitude_1,
             a2.step_id,
             a2.latitude AS latitude_2,
             a2.longitude AS longitude_2,
             rank() OVER (PARTITION BY a1.user_id,
                                       a1.session_id,
                                       a1.day
                          ORDER BY a2.step_id-a1.step_id DESC)
      FROM google_fit_location a1
      JOIN google_fit_location a2 ON a1.user_id = a2.user_id
      AND a1.session_id=a2.session_id
      AND a1.day = a2.day
      WHERE a2.step_id > a1.step_id) x
   WHERE rank =1) y


-- WHERE STUDENTS ARE AT THE MEDIAN
SELECT student_id
FROM sat_scores
WHERE sat_writing = 
        (SELECT 
               percentile_disc(0.5) within group (order by sat_writing)
         FROM sat_scores)
ORDER BY 1 DESC;


-- TRICKY. using window function to assigne values in a sub query, the call them 
SELECT games, SUM(old_count), SUM(young_count), SUM(old_count) :: NUMERIC /  SUM(young_count) as old_young_ratio
FROM(
SELECT distinct games as games, name,
       (CASE WHEN age <= 25 THEN 1 ELSE 0 END) as young_count, 
       (CASE WHEN age >= 50 THEN 1 ELSE 0 END) as old_count
FROM olympics_athletes_events	
) t
GROUP BY 1;


SELECT (MAX(sum_score) - min(sum_score)) as score_difference
FROM(
SELECT student, SUM(assignment1 + assignment2 + assignment3) as sum_score
FROM box_scores

