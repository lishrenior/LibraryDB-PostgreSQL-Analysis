
/* Q1 CHECK Constraint (1 mark)
Most of the column-level constraints for the schema are simple NOT NULL, primary-key or
foreign-key constraints which are best expressed as part of the table definition. However,
sometimes it is useful to name a constraint, which means it can be defined separately to
the table.
There are four permitted values of event_type: Loan, Return, Hold and Loss.
Write a constraint (named CK_EVENT_TYPE) to enforce this requirement.*/
--alter table Events
ALTER TABLE Events
ADD CONSTRAINT CK_EVENT_TYPE
CHECK(event_type IN('Loan', 'Return', 'Hold', 'Loss'));
/* Q2.1 (2 marks)
Write a trigger named BI_GUARDIAN and accompanying user-defined function
named UDF_BI_GUARDIAN to ensure that all new patrons who are children (under 18
years old) have a guardian who is an existing patron.*/
--create function
CREATE OR REPLACE FUNCTION UDF_BI_GUARDIAN() RETURNS TRIGGER AS $$
BEGIN
--check age > 18
IF(NEW.dob > CURRENT_DATE - INTERVAL '18 years') THEN
--check if guardian exist
IF(NEW.guardian IS NULL OR NOT EXISTS(SELECT 1 FROM Patrons WHERE patron_id = NEW.guardian)) THEN
RAISE EXCEPTION 'Patrons under 18 years old must have a guardian.';
END IF;
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--create trigger
CREATE TRIGGER BI_GUARDIAN
BEFORE INSERT ON Patrons
FOR EACH ROW
EXECUTE FUNCTION UDF_BI_GUARDIAN();
/* Q2.2 (2 marks)
Write a trigger named BI_EMAIL_ADDR and accompanying user-defined function
named UDF_BI_EMAIL_ADDR to ensure that all new patrons who are adults (18 years
and older) have an email address.*/
--create function
CREATE OR REPLACE FUNCTION UDF_BI_EMAIL_ADDR() RETURNS TRIGGER AS $$
BEGIN
--check patron's age > 18
IF (NEW.dob <= CURRENT_DATE - INTERVAL '18 years') THEN
--make sure email provided
IF(NEW.email_address IS NULL OR NEW.email_address = '')THEN
RAISE EXCEPTION 'Adults patrons must have email address.';
END IF;
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--create trigger
CREATE TRIGGER BI_EMAIL_ADDR
BEFORE INSERT ON Patrons
FOR EACH ROW
EXECUTE FUNCTION UDF_BI_EMAIL_ADDR();
/* Q3.1 Sequence creation (1 mark)
Create an integer sequence named ITEM_ID_SEQ for the Items table. The minimum
value is 1000000000, the maximum is 9999999999 and the increment is 1.*/
--create sequence Q3.1
CREATE SEQUENCE ITEM_ID_SEQ
MINVALUE 1000000000
MAXVALUE 9999999999
INCREMENT BY 1
START WITH 1000000000
;
/* Q3.2 Sequences and Triggers (2 marks)
Create a trigger named BI_ITEM_ID and accompanying user-defined function
named UDF_BI_ITEM_ID to populate the primary keys for new Items as described
above. */
--create UDF
CREATE OR REPLACE FUNCTION UDF_BI_ITEM_ID() RETURNS TRIGGER AS $$
DECLARE
seq_number bigint;
barcode_text varchar;
checksum_digit int;
i int;
sum_digit int := 0;
BEGIN
--get next value from sequence

seq_number := nextval('ITEM_ID_SEQ');
--calculate sum of all digit in seq_number % 10
FOR i IN 1..10 LOOP
sum_digit := sum_digit + (substring(seq_number::text, i, 1)::int);
END LOOP;
checksum_digit := sum_digit % 10;
--barcode text 'UQ' prefix, seq_number, checksum digit
barcode_text := 'UQ' || seq_number || checksum_digit;
--assign barcode text to PK of new record
NEW.item_id := barcode_text;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--create trigger
CREATE TRIGGER BI_ITEM_ID
BEFORE INSERT ON Items
FOR EACH ROW
EXECUTE FUNCTION UDF_BI_ITEM_ID();
/* Q3.3 Sequence identification (1 mark)
Write a query to list the Postgres-internal sequence(s) created via the use of serial
on the Patrons and Events tables. */
--sequence identification
SELECT
c.relname AS sequence_name
FROM
pg_class c
JOIN
pg_namespace n ON n.oid = c.relnamespace
WHERE
c.relkind = 'S'
AND n.nspname = 'public'
AND(
c.relname LIKE 'patrons_%_seq' OR
c.relname LIKE 'events_%_seq'
);
/* Q4.1 Losses (2 marks)
Sometimes an item is permanently lost by a patron. When this happens, they need
to be charged the cost of its replacement, which is stored in the cost field of the
relevant row in the Works table.
Write a trigger BI_LOSS_CHARGE and accompanying user-defined function
UDF_BI_LOSS_CHARGE to populate the charge field when a new Loss is inserted into
Events.*/
--create UDF for loss charge
CREATE OR REPLACE FUNCTION UDF_BI_LOSS_CHARGE() RETURNS TRIGGER AS $$
BEGIN
--check 'Loss'
IF(NEW.event_type = 'Loss') THEN
--charge cost
SELECT cost INTO NEW.charge
FROM Works
WHERE isbn = (SELECT isbn FROM Items WHERE item_id = NEW.item_id);
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--trigger for loss charge
CREATE TRIGGER BI_LOSS_CHARGE
BEFORE INSERT ON Events
FOR EACH ROW
EXECUTE FUNCTION UDF_BI_LOSS_CHARGE();
/* Q4.2 Missing Returns (4 marks)
Sometimes, an item may be improperly returned (such that the Return event is not
recorded in the database) and then re-borrowed by another patron. In this case, the
library system needs to credit the previous borrower with its return.
Write a trigger AI_MISSING_RETURN (and accompanying user-defined function
UDF_AI_MISSING_RETURN) that detects this situation on a new Loan event and
subsequently inserts a Return for the previous borrower, timestamped to one hour
earlier.
(3 marks for correctly inserting returns across three different circulation-history
scenarios; 1 mark for not inserting spurious returns.) */
--create function fro missing return
CREATE OR REPLACE FUNCTION public.udf_ai_missing_return()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
last_loan RECORD;
BEGIN
--check even = 'Loan'

IF(NEW.event_type = 'Loan') THEN
--find the same item id, the most recent 'Loan'
SELECT * INTO last_loan
FROM Events e1
WHERE e1.item_id = NEW.item_id
AND e1.event_type = 'Loan'
AND e1.time_stamp <> NEW.time_stamp
ORDER BY e1.time_stamp DESC
LIMIT 1;
--if loan is found, and different borrower, return previous borrower, 1 hr earlier
IF FOUND AND last_loan.patron_id <> NEW.patron_id THEN
-- Insert a 'Return' event 1 hour before the new 'Loan'
PERFORM pg_notify('insert_return', 'Skipping trigger execution');
INSERT INTO Events (patron_id, item_id, event_type, time_stamp, charge)
VALUES (last_loan.patron_id, last_loan.item_id, 'Return', NEW.time_stamp - INTERVAL '1 hour', NULL);
END IF;
END IF;
RETURN NEW;
END;
$function$;
--trigger for missing return
CREATE TRIGGER AI_MISSING_RETURN
AFTER INSERT ON Events
FOR EACH ROW
WHEN (NEW.event_type = 'Loan')
EXECUTE FUNCTION public.udf_ai_missing_return();
/* Q4.3 Holds (5 marks)
Patrons may request a hold on an item. For this event type, the time_stamp should
represent the expiry of the hold, rather than the time the hold was initially placed.
A hold may only be placed on an item if it is [a] not already held by any patron AND
either ([b] available for lending, OR [c] currently on loan to a different patron).
Otherwise, the hold must be rejected. (3 marks).
If a held item is currently on loan, the expiry of the hold is 42 days after its loan
timestamp. Otherwise, the expiry of the hold is 14 days from the current time (2
marks).
Write a trigger BI_HOLDS (and accompanying user-defined function UDF_BI_HOLDS)
to implement this functionality by rejecting the insert (if appropriate) or by setting
the value of time_stamp. */
--function handle hold logic
CREATE OR REPLACE FUNCTION public.udf_bi_holds()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
LAST_LOAN_TIME TIMESTAMP;
BEGIN
--check if item is loss
IF EXISTS(
SELECT 1 FROM EVENTS
WHERE ITEM_ID = NEW.ITEM_ID AND EVENT_TYPE = 'Loss'
)THEN
RAISE EXCEPTION 'Item is loss, cannot be hold.';
END IF;
--check if item is on hold, not returned
IF EXISTS(
SELECT 1
FROM EVENTS
WHERE ITEM_ID = NEW.ITEM_ID
AND EVENT_TYPE = 'Hold'
AND NOT EXISTS(
SELECT 1
FROM EVENTS e2
WHERE e2.ITEM_ID = NEW.ITEM_ID
AND e2.EVENT_TYPE = 'Return'
AND e2.time_stamp > EVENTS.time_stamp
)
)THEN
--if return event after hold 
RETURN NEW;
--check if it's on loan by another patron
ELSIF EXISTS (
SELECT 1 
FROM EVENTS
WHERE ITEM_ID = NEW.ITEM_ID
AND EVENT_TYPE = 'Loan'
AND PATRON_ID <> NEW.PATRON_ID
) THEN
--expiry of hold 42 days after loan

SELECT MAX(time_stamp) INTO LAST_LOAN_TIME
FROM EVENTS
WHERE ITEM_ID = NEW.ITEM_ID
AND EVENT_TYPE = 'Loan';
NEW.TIME_STAMP := LAST_LOAN_TIME + INTERVAL '42 days';
--set hold time to 14 days if none
 ELSIF EXISTS (
SELECT 1 
FROM EVENTS e1
WHERE e1.ITEM_ID = NEW.ITEM_ID
AND e1.EVENT_TYPE = 'Hold'
AND NOT EXISTS (
SELECT 1 
FROM EVENTS e2
WHERE e2.ITEM_ID = NEW.ITEM_ID
AND e2.EVENT_TYPE = 'Return'
AND e2.time_stamp > e1.time_stamp
)
) THEN
RAISE EXCEPTION 'Item is currently on hold.' ;
--if none of above, set the hold time to 14 days
 ELSE
NEW.TIME_STAMP := NEW.TIME_STAMP + INTERVAL '14 days';
END IF;
RETURN NEW;
END;
$function$;
--trigger for hold
CREATE TRIGGER BI_HOLDS
BEFORE INSERT ON EVENTS
FOR EACH ROW
WHEN (NEW.EVENT_TYPE = 'Hold')
EXECUTE FUNCTION UDF_BI_HOLDS();
