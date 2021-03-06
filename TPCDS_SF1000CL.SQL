-- SNOWFLAKE_SAMPLE_DATA.TPCDS_SF1000CL 
-- PRACTICE PROBLEMS


-- query_1
-- Per Country, what is the sum price for each order status. 
SELECT n.n_name, o.o_orderstatus, sum(o.o_totalprice) as total_price
FROM customer c
JOIN orders o 
    ON c.c_custkey = o.o_custkey
JOIN nation n
    ON c.c_nationkey = n.n_nationkey
GROUP BY 1,2
ORDER BY 1

-- Sub query for further analysis
SELECT o_orderstatus, STDDEV(total_price)
FROM(
     SELECT n.n_name, o.o_orderstatus, sum(o.o_totalprice) as total_price
     FROM customer c
     JOIN orders o 
         ON c.c_custkey = o.o_custkey
     JOIN nation n
         ON c.c_nationkey = n.n_nationkey
     GROUP BY 1,2
     ORDER BY 1)
GROUP BY 1

-- query_2
-- total amount in supply per brand, per manufacturer
SELECT p.p_mfgr,p.p_brand,AVG(ps.ps_availqty * ps.ps_supplycost) as average_total_supply_amount
FROM part p
JOIN partsupp ps
   on p.p_partkey = ps.ps_partkey
GROUP BY 1,2
ORDER BY 1,2


-- query 3
-- count how many comments have slyly in them
WITH cte as 
         (SELECT ps_partkey, ps_comment
         FROM partsupp
         WHERE ps_comment LIKE '%slyly%')
SELECT count(ps_comment)
FROM cte


-- query 4 
-- top selling parts
WITH sum_rev as 
     (SELECT l.l_partkey, SUM(o.o_totalprice) as total_price
      FROM lineitem l
      JOIN orders o
          on l.l_orderkey = o.o_orderkey
      GROUP BY 1),
ranking_sum as (SELECT l_partkey, total_price, dense_rank() 
                     over(order by total_price DESC) as rnk
                    FROM sum_rev) 
SELECT l_partkey, total_price, rnk
FROM ranking_sum
WHERE rnk <= 10