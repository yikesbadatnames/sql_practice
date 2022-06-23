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