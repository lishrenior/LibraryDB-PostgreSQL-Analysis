/*Q1.1 View A (1 mark)
The librarians would like to confirm the most popular genres.
Create a view V_POPULAR_GENRES to determine the five most popular genres, as determined
by number of times an item has been borrowed or held.
Query the view (SELECT * FROM V_POPULAR_GENRES;) and report the results.
Report and explain the query plan. You should refer to the reported times, costs, the number
of rows fetched, your knowledge of query planning (and anything else you feel is relevant).*/
--REF: https://www.youtube.com/watch?v=Mll5SqR4RYk
--confirm the most popular genres
CREATE VIEW V_POPULAR_GENRES AS
SELECT W.genre, COUNT(*) AS popularity_count
FROM Works W
JOIN Items I ON W.isbn = I.isbn
JOIN Events E ON I.item_id = E.item_id
WHERE E.event_type IN ('Loan', 'Hold')
GROUP BY w.GENRE
ORDER BY popularity_count DESC
--determine 5
LIMIT 5;
--select view
SELECT * FROM V_POPULAR_GENRES;
--explain analyze view
EXPLAIN ANALYZE SELECT * FROM V_POPULAR_GENRES;

/*Q1.2 View B (2 marks)
When a child loses an item, the cost is charged to their account but their guardian is
responsible for paying it.
Create a view V_COSTS_INCURRED to identify the five patrons who are responsible for paying
the most charges incurred from January through June 2024.
Hint: consider using Common Table Expressions to build up your query in a semantically
meaningful way.*/
--create view
CREATE VIEW V_COSTS_INCURRED AS
WITH LostItems As (
SELECT P.guardian, E.charge
FROM Patrons P
JOIN Events E ON P.patron_id = E.patron_id
WHERE E.event_type = 'Loss'
AND E.time_stamp BETWEEN '2024-01-01' AND '2024-06-30'
AND P.guardian IS NOT NULL
), GuardianCharges AS(
-- sum charges for each guardian
SELECT guardian, SUM(charge) AS total_charge
FROM LostItems
GROUP BY guardian
)
--return 5 responsible patrons
SELECT guardian, total_charge
FROM GuardianCharges
ORDER BY total_charge DESC
LIMIT 5;

/*Q1.3 Materialised view (2 marks)
1. Create a materialised view using the same query as V_COSTS_INCURRED, called
MV_COSTS_INCURRED. (0 marks)
2. Report, compare and explain the two following queries, which select the contents
of the view and materialised view, respectively. (2 marks)
Query A:
SELECT * FROM V_COSTS_INCURRED;
Query B:
SELECT * FROM MV_COSTS_INCURRED;
You should refer to the reported times, costs, the number of rows fetched, your knowledge
of query planning (and anything else you feel is relevant).*/
CREATE MATERIALIZED VIEW MV_COSTS_INCURRED AS
WITH LostItems As (
SELECT P.guardian, E.charge
FROM Patrons P
JOIN Events E ON P.patron_id = E.patron_id
WHERE E.event_type = 'Loss'
AND E.time_stamp BETWEEN '2024-01-01' AND '2024-06-30'
AND P.guardian IS NOT NULL
), GuardianCharges AS(
-- sum charges for each guardian
SELECT guardian, SUM(charge) AS total_charge
FROM LostItems
GROUP BY guardian
)
--return 5 responsible patrons
SELECT guardian, total_charge
FROM GuardianCharges
ORDER BY total_charge DESC
LIMIT 5;

--select view
SELECT * FROM V_COSTS_INCURRED;
EXPLAIN ANALYZE SELECT * FROM V_COSTS_INCURRED;
--select materialized view
SELECT * FROM MV_COSTS_INCURRED;
EXPLAIN ANALYZE SELECT * FROM MV_COSTS_INCURRED;

/*Q2.1 Basic index (2 marks)
(1) Create an index (IDX_EVENT_ITEM) on the event_type and item_id fields of the Events
table. (0.5 marks)
(2) Re-run your query from Q1.1 again (SELECT * FROM V_POPULAR_GENRES;). Report the
query plan. Compare this query plan to those from Q1.1 and explain the differences,
if any. (1.5 marks)*/
--create index
CREATE INDEX IDX_EVENT_ITEM ON Events (event_type, item_id);
--select view
SELECT * FROM V_POPULAR_GENRES;
--explain anylyze select view
EXPLAIN ANALYZE SELECT * FROM V_POPULAR_GENRES;

/*Q2.2 Function-based index (4 marks)
Due to a previous decision in the database design, author’s names are stored as a single
field. However, for library purposes the surname of the author is relevant for deriving the
call number. For this purpose, the surname is defined as the last word (space-delimited) in
the author field.
(1) Create an expression to identify each author’s surname and present the results. You
may use string processing functions, including the regex functions. (1 mark)
(2) Report and explain the query plan. You should refer to the reported times, costs, the
number of rows fetched, your knowledge of query planning (and anything else you
feel is relevant). (1 mark)
(3) Create a function-based index to potentially speed up queries on this expression.
(0.5 marks)
(4) Report the query plan with the function-based index in place. Explain the differences
to the previous plan, if any. You should refer to the reported times, costs, the number
of rows fetched, your knowledge of query planning (and anything else you feel is
relevant). (1.5 marks)
Bonus content: in real libraries, books are assigned a call number derived from the subject
matter and authors’ surname. Books are stored on the shelves in call number order. This
makes call numbers an example of a physical indexing scheme!*/
--identify surname
SELECT
author,
regexp_replace(author, '^.*\s', '')AS surname
from works;
--explain analyze
EXPLAIN ANALYZE SELECT
author,
regexp_replace(author, '^.*\s', '')AS surname
from works;
--index to speed up queries
CREATE INDEX IDX_AUTHOR_SURNAME
ON WORKS ((REGEXP_REPLACE(AUTHOR, '^.*\s', '')));
--explain analyze
EXPLAIN ANALYZE SELECT
author,
regexp_replace(author, '^.*\s', '')AS surname
from works;

/*Q3 Indexes and query planning (4 marks)
PostgreSQL allows you to influence the query planner with runtime configuration
parameters in psql.
https://www.postgresql.org/docs/16/runtime-config-query.html
In this section we will suppress the use of several different scan types to compare queries.
Report and explain the execution plan for the following two queries (A & B):
(1) with both index and sequential scans enabled;
(2) with index scans enabled and sequential scans suppressed;
(3) with index scans suppressed and sequential scans enabled.
Explain why the query planner might choose one method over another.
You should refer to the reported times and costs from the execution plan, the number of
rows fetched, your knowledge of query planning (and anything else you feel is relevant).
Note: For this question, bitmap and index-only scans count as index scans too.*/
--(1) both enabled
SET enable_seqscan = on;
SET enable_indexscan = on;
--select
SELECT * FROM EVENTS WHERE EVENT_ID < 100;
--explain analyze
EXPLAIN ANALYZE SELECT * FROM EVENTS WHERE EVENT_ID < 100;
SELECT * FROM EVENTS WHERE EVENT_ID >= 100;
--explain analyze
EXPLAIN ANALYZE SELECT * FROM EVENTS WHERE EVENT_ID >= 100;

--reset
RESET enable_seqscan;
RESET enable_indexscan;

--(2) index scans enabled and sequential scans suppressed
SET enable_seqscan = off;
SET enable_indexscan = on;
--select
SELECT * FROM EVENTS WHERE EVENT_ID < 100;
--explain analyze
EXPLAIN ANALYZE SELECT * FROM EVENTS WHERE EVENT_ID < 100;
SELECT * FROM EVENTS WHERE EVENT_ID >= 100;
--explain analyze
EXPLAIN ANALYZE SELECT * FROM EVENTS WHERE EVENT_ID >= 100;

--reset
RESET enable_seqscan;
RESET enable_indexscan;

--(3) index scans suppressed and sequential scans enabled
SET enable_seqscan = on;
SET enable_indexscan = off;
--select
SELECT * FROM EVENTS WHERE EVENT_ID < 100;
--explain analyze
EXPLAIN ANALYZE SELECT * FROM EVENTS WHERE EVENT_ID < 100;
SELECT * FROM EVENTS WHERE EVENT_ID >= 100;
--explain analyze
EXPLAIN ANALYZE SELECT * FROM EVENTS WHERE EVENT_ID >= 100;
--reset
RESET enable_seqscan;
RESET enable_indexscan;

/*Q4 Transactions (4 marks)
In this question we will examine what happens when transactions occur concurrently.
(1) In a psql connection, begin a transaction and then perform a query to identify an item
which is currently returned. (1.5 marks)
(2) In a second psql connection, begin another transaction and attempt to record a hold
on that item for some patron 14 days in the future, then commit. (0.5 marks)
(3) In your first connection, attempt to record a loan on that item for a different patron
(at the present time), then commit. (0.5 marks)*/
--first connection
BEGIN; --START TRANSACTION 
--THIS PART WOULD TAKE A FEW MINUTES TO DISPLAY RESULT
SELECT I.*                                                                       
FROM ITEMS I                                                                            
JOIN EVENTS E ON I.ITEM_ID = E.ITEM_ID                                                  
WHERE E.EVENT_TYPE = 'Return'                                                           
AND E.TIME_STAMP = (                                                                    
SELECT MAX(E2.TIME_STAMP)                                                               
FROM EVENTS E2                                                                           
WHERE E2.ITEM_ID =I.ITEM_ID                                                             
);  

--second connection
BEGIN; --START
INSERT INTO events (patron_id, item_id, event_type, time_stamp)                     
VALUES (10, 'UQ10000154786', 'Hold', NOW() + INTERVAL '14 days'); 
COMMIT;

--first connection
INSERT INTO EVENTS (PATRON_ID, ITEM_ID, EVENT_TYPE, TIME_STAMP)  
VALUES( 100, 'UQ10000148790', 'Loan', NOW());  
COMMIT;