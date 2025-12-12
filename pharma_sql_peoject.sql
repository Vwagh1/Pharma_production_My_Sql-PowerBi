create database pharma;
use pharma;

-- Cleaning Data ---
-- table pharma_machine_logs ---
-- First Understand the Data Structure
DESCRIBE pharma_machine_logs;

-- Preview the Data --
select * from pharma_machine_logs
limit 10;

-- safe update mode
SET SQL_SAFE_UPDATES = 0;

-- convert coloum lower to upper
update pharma_machine_logs
set Maintenance_Required = upper(trim(Maintenance_Required));

-- convert date into date format
alter table pharma_machine_logs
modify Log_Date date;

-- Check for missing or null values
select
	sum(case when machine_id is null then 1 else 0 end) as null_machine_id,
	sum(case when runtime_hrs is null then 1 else 0 end) as null_runtime_id,
	sum(case when downtime_hrs is null then 1 else 0 end) as null_downtime_hrs,
	sum(case when maintenance_required is null then 1 else 0 end) as null_log_date,
	sum(case when log_date is null then 1 else 0 end) as null_log_date
from pharma_machine_logs; -- this query is showing there is no null value but have balnk, blank (“”) values are not the same as NULL.

-- Detect Blank (Empty String) Values
SELECT 
    SUM(CASE WHEN machine_id IS NULL OR Machine_ID = '' THEN 1 ELSE 0 END) AS blank_machine_id,
    SUM(CASE WHEN runtime_hrs IS NULL OR Runtime_hrs = '' THEN 1 ELSE 0 END) AS blank_runtime,
    SUM(CASE WHEN Downtime_hrs IS NULL OR Downtime_hrs = '' THEN 1 ELSE 0 END) AS blank_downtime, -- have 70 null -
    SUM(CASE WHEN Maintenance_Required IS NULL OR Maintenance_Required = '' THEN 1 ELSE 0 END) AS blank_maintenance, -- have 1975 null - Blank often means maintenance not required.
    SUM(CASE WHEN Log_Date IS NULL OR CAST(Log_Date AS CHAR) = '' THEN 1 ELSE 0 END) AS blank_log_date
FROM pharma_machine_logs;

-- replace downtime with 0 becouse If downtime dont recorded, its likely 0 hours downtime — not missing machinery.
UPDATE pharma_machine_logs
SET Downtime_hrs = 0
WHERE Downtime_hrs IS NULL OR Downtime_hrs = ''; -- dont able to update becouse but showing 70 nulls record 

		/* The column is of numeric type (DOUBLE), so empty string '' was automatically stored as 0 by MySQL during data import.
		→ So technically there are no NULLs or blanks anymore, just 0 values.
		→ That’s why MySQL says “matched 70, changed 0” — the data didn’t need to change. */
        
SELECT DISTINCT Downtime_hrs FROM pharma_machine_logs ORDER BY Downtime_hrs;


-- replace blank_maitenance with 'NO' becouse Blank often means maintenance not required.(from my assumstion)
UPDATE pharma_machine_logs
SET Maintenance_Required = 'NO'
WHERE Maintenance_Required IS NULL OR Maintenance_Required = '';

--  Check and Fix Outliers (optional but useful)
SELECT * FROM pharma_machine_logs
WHERE Runtime_hrs < 0 OR Downtime_hrs < 0 OR Runtime_hrs > 24;

-- Validate and Format Dates
SELECT Log_Date FROM pharma_machine_logs WHERE Log_Date IS NULL OR Log_Date = '';
SELECT 
    SUM(CASE WHEN Log_Date IS NULL THEN 1 ELSE 0 END) AS null_log_date
FROM pharma_machine_logs;

-- check dublicate values
SELECT machine_id , COUNT(*) AS duplicate_count
FROM pharma_machine_logs
GROUP BY machine_id
HAVING COUNT(*) > 1;





-- ---------------------             table pharma_employees           ----------------------

-- Check for Missing or Blank Values
SELECT COUNT(*) AS total_rows
FROM pharma_employees;


-- Check for NULL or Blank Values
SELECT 
    SUM(CASE WHEN Emp_ID IS NULL OR Emp_ID = '' THEN 1 ELSE 0 END) AS blank_emp_id,
    SUM(CASE WHEN Emp_Name IS NULL OR Emp_Name = '' THEN 1 ELSE 0 END) AS blank_name,
    SUM(CASE WHEN Department IS NULL OR Department = '' THEN 1 ELSE 0 END) AS blank_department,
    SUM(CASE WHEN Experience_Years IS NULL THEN 1 ELSE 0 END) AS null_experience,
    SUM(CASE WHEN Salary IS NULL THEN 1 ELSE 0 END) AS null_salary
FROM pharma_employees;

-- Standardize Text Columns
UPDATE pharma_employees
SET Emp_Name = UPPER(TRIM(Emp_Name));

UPDATE pharma_employees
SET Department = UPPER(TRIM(Department));

-- Check Outliers or Negative Values
SELECT *
FROM pharma_employees
WHERE Experience_Years < 0 OR Salary < 0;


-- ---------------------- FULL DATA CLEANING PROCESS – pharma_production_batches -----------------------
SELECT COUNT(*) AS total_rows
FROM pharma_production_batches;

SELECT 
    SUM(CASE WHEN Batch_ID IS NULL OR Batch_ID = '' THEN 1 ELSE 0 END) AS blank_batch_id,
    SUM(CASE WHEN Product_Name IS NULL OR Product_Name = '' THEN 1 ELSE 0 END) AS blank_product,
    SUM(CASE WHEN Production_Date IS NULL OR CAST(Production_Date AS CHAR) = '' THEN 1 ELSE 0 END) AS blank_date,
    SUM(CASE WHEN Shift IS NULL OR Shift = '' THEN 1 ELSE 0 END) AS blank_shift,
    SUM(CASE WHEN Operator_Name IS NULL OR Operator_Name = '' THEN 1 ELSE 0 END) AS blank_operator,
    SUM(CASE WHEN Machine_ID IS NULL OR Machine_ID = '' THEN 1 ELSE 0 END) AS blank_machine,
    SUM(CASE WHEN Temperature_C IS NULL THEN 1 ELSE 0 END) AS null_temperature,
    SUM(CASE WHEN Pressure_bar IS NULL THEN 1 ELSE 0 END) AS null_pressure,
    SUM(CASE WHEN `Yield_%` IS NULL THEN 1 ELSE 0 END) AS null_yield,
    SUM(CASE WHEN Cost_per_Batch IS NULL THEN 1 ELSE 0 END) AS null_cost,
    SUM(CASE WHEN Quality_Status IS NULL OR Quality_Status = '' THEN 1 ELSE 0 END) AS blank_quality
FROM pharma_production_batches;


-- High importance — because product type affects yield, cost, and quality,product name have 277 blank , i can replace with 'unknown product name '
UPDATE pharma_production_batches 
SET Product_Name = 'UNKNOWN PRODUCT' 
WHERE Product_Name IS NULL OR Product_Name = '';

-- operator_name have 301 blank Importance Medium — useful for productivity analysis, but not always critical for yield or cost metrics. Replace with 'UNASSIGNED'
UPDATE pharma_production_batches 
SET Operator_Name = 'UNASSIGNED'
WHERE Operator_Name IS NULL OR Operator_Name = '';

-- Quality_Status have 1959 blanks Importance is Very High — this is your target variable for performance or quality monitoring.
UPDATE pharma_production_batches 
SET Quality_Status = 'PENDING'
WHERE Quality_Status IS NULL OR Quality_Status = '';

-- Convert Production_Date to DATE Format
ALTER TABLE pharma_production_batches
MODIFY COLUMN Production_Date DATE;

-- Standardize Text Columns
UPDATE pharma_production_batches
SET 
    Product_Name = UPPER(TRIM(Product_Name)),
    Shift = UPPER(TRIM(Shift)),
    Operator_Name = UPPER(TRIM(Operator_Name)),
    Machine_ID = UPPER(TRIM(Machine_ID)),
    Quality_Status = UPPER(TRIM(Quality_Status));

-- Detect and Fix Outliers
SELECT * 
FROM pharma_production_batches
WHERE Temperature_C < 0 OR Pressure_bar < 0 OR 'Yield_%' > 100 OR 'Yield_%' < 0 OR Cost_per_Batch < 0;

UPDATE pharma_production_batches SET Temperature_C = NULL WHERE Temperature_C < 0;
UPDATE pharma_production_batches SET Pressure_bar = NULL WHERE Pressure_bar < 0;
UPDATE pharma_production_batches SET `Yield_%` = NULL WHERE `Yield_%` < 0 OR `Yield_%` > 100;
UPDATE pharma_production_batches SET Cost_per_Batch = NULL WHERE Cost_per_Batch < 0;


-- --------------------------- data cleaning of pharma_quality_checks  -------------------------------
SELECT COUNT(*) AS total_rows FROM pharma_quality_checks;

-- Check for NULL or blank values in each column
SELECT 
    SUM(CASE WHEN Batch_ID IS NULL OR Batch_ID = '' THEN 1 ELSE 0 END) AS blank_batch_id,
    SUM(CASE WHEN pH IS NULL THEN 1 ELSE 0 END) AS null_ph,
    SUM(CASE WHEN `Moisture_%` IS NULL THEN 1 ELSE 0 END) AS null_moisture,
    SUM(CASE WHEN Defect_Type IS NULL OR Defect_Type = '' THEN 1 ELSE 0 END) AS blank_defect_type, -- have 2819 blank values
    SUM(CASE WHEN Checked_By IS NULL OR Checked_By = '' THEN 1 ELSE 0 END) AS blank_checked_by,
    SUM(CASE WHEN QC_Date IS NULL OR QC_Date = '' THEN 1 ELSE 0 END) AS blank_qc_date
FROM pharma_quality_checks;


-- Replace blank Defect_Type with "NO DEFECT"
UPDATE pharma_quality_checks
SET Defect_Type = 'NO DEFECT'
WHERE Defect_Type IS NULL OR Defect_Type = '';


-- Standardize text columns
UPDATE pharma_quality_checks
SET 
    Batch_ID = TRIM(UPPER(Batch_ID)),
    Defect_Type = TRIM(UPPER(Defect_Type)),
    Checked_By = TRIM(UPPER(Checked_By));

ALTER TABLE pharma_quality_checks
MODIFY QC_Date DATE;



-- going to stablish relationship between all tables
select emp_id , count(*)
from pharma_employees
group by emp_id
having count(*) > 1;

alter table pharma_employees
modify emp_id varchar(50);

alter table pharma_employees
modify emp_name varchar(50);

alter table pharma_employees
modify department varchar(50);

alter table pharma_employees
add constraint primary key(emp_id);

-- 
select machine_id , count(*)
from pharma_machine_logs
group by machine_id
having count(*) > 1;

alter table pharma_machine_logs
modify machine_id varchar(50);

SELECT Machine_ID, Runtime_hrs, log_date, COUNT(*) AS duplicate_count
FROM pharma_machine_logs
GROUP BY Machine_ID, Runtime_hrs , log_date
HAVING COUNT(*) > 1;

alter table pharma_machine_logs
add constraint pk_machine_log primary key(machine_id , runtime_hrs, log_date );

alter table pharma_machine_logs
modify Maintenance_Required varchar(5);


-- 
alter table pharma_production_batches
modify batch_id varchar(20);

SELECT batch_id, COUNT(*) AS duplicate_count
FROM pharma_production_batches
GROUP BY batch_id
HAVING COUNT(*) > 1;

alter table pharma_production_batches
add constraint primary key(batch_id);

alter table pharma_production_batches
modify product_name varchar(50);

alter table pharma_production_batches
modify shift varchar(15);

alter table pharma_production_batches
modify product_name varchar(50);

alter table pharma_production_batches
modify operator_name varchar(50);

alter table pharma_production_batches
modify machine_id varchar(10);

alter table pharma_production_batches
modify quality_status varchar(10);

-- 
alter table pharma_quality_checks
modify batch_id varchar(15);

alter table pharma_quality_checks
modify Defect_Type varchar(15);

alter table pharma_quality_checks
modify checked_by varchar(15);

--
-- Verify the Imported Data
-- Check first few rows
SELECT * FROM pharma_production_batches LIMIT 5;
SELECT * FROM pharma_quality_checks LIMIT 5;
SELECT * FROM pharma_machine_logs LIMIT 5;
SELECT * FROM pharma_employees LIMIT 5;

-- Count total rows in each table
SELECT COUNT(*) AS total_batches FROM pharma_production_batches;
SELECT COUNT(*) AS total_quality_checks FROM pharma_quality_checks;
SELECT COUNT(*) AS total_machine_logs FROM pharma_machine_logs;
SELECT COUNT(*) AS total_employees FROM pharma_employees;

-- 1. Trim spaces and standardize case
UPDATE pharma_production_batches
SET Quality_Status = UPPER(TRIM(Quality_Status));

-- 2. Handle NULL values
UPDATE pharma_production_batches
SET `Yield_%` = 0
WHERE `Yield_%` IS NULL;

--
select batch_id , count(*)
from pharma_quality_checks
group by batch_id
having count(*) > 1;

alter table pharma_quality_checks
modify batch_id varchar(15);

alter table pharma_quality_checks
add constraint primary key(batch_id);

alter table pharma_quality_checks
add constraint foreign key(batch_id) references pharma_production_batches(batch_id);

SELECT p.Operator_Name, e.Emp_Name
FROM pharma_production_batches p
LEFT JOIN pharma_employees e
    ON p.Operator_Name = e.Emp_Name;

SELECT q.Checked_By, e.Emp_Name
FROM pharma_quality_checks q
LEFT JOIN pharma_employees e
    ON q.Checked_By = e.Emp_Name;

ALTER TABLE pharma_employees
CHANGE Emp_Name Employee_Name varchar(15);

ALTER TABLE pharma_production_batches
ADD INDEX idx_operator (Operator_Name);

ALTER TABLE pharma_quality_checks
ADD INDEX idx_checker (Checked_By);


ALTER TABLE pharma_machine_logs 
ADD INDEX idx_machine_logs_machine (machine_id);

ALTER TABLE pharma_production_batches
ADD INDEX idx_prod_batches_machine (machine_id);

alter table pharma_machine_logs
add index idx_downtime_machin (downtime_hrs);

alter table pharma_production_batches
add index idx_yield_production (`Yield_%`);

SELECT 
    m.machine_id,
    SUM(m.downtime_hrs) AS total_downtime,
    AVG(p.`Yield_%`) AS avg_yield
FROM pharma_machine_logs AS m
STRAIGHT_JOIN pharma_production_batches AS p
    ON m.machine_id = p.machine_id
GROUP BY m.machine_id
HAVING avg_yield < 70
ORDER BY total_downtime DESC;


SET GLOBAL max_allowed_packet = 1024*1024*256;
SET GLOBAL net_read_timeout = 120;
SET GLOBAL net_write_timeout = 120;
SET GLOBAL wait_timeout = 600;
SET GLOBAL interactive_timeout = 600;


alter table pharma_machine_logs
drop primary key;

ALTER TABLE pharma_machine_logs
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;


ALTER TABLE pharma_machine_logs                    
ADD INDEX idx_pml_machine (machine_id),
ADD INDEX idx_pml_logdate (log_date);


ALTER TABLE pharma_production_batches
ADD INDEX idx_ppb_machine (machine_id),
ADD INDEX idx_ppb_batchid (batch_id),
ADD INDEX idx_ppb_productiondate (production_date),
ADD INDEX idx_ppb_shift (shift),
ADD INDEX idx_ppb_quality (quality_status);

ALTER TABLE pharma_quality_checks
ADD INDEX idx_pqc_batchid (batch_id),
ADD INDEX idx_pqc_qcdate (qc_date),
ADD INDEX idx_pqc_checkedby (checked_by);

ALTER TABLE pharma_employees
ADD INDEX idx_emp_id (emp_id),
ADD INDEX idx_emp_dept (department),
ADD INDEX idx_emp_name (employee_name);


ALTER TABLE pharma_production_batches
ADD CONSTRAINT FOREIGN KEY(machine_id)
REFERENCES pharma_machine_logs(machine_id);

ALTER TABLE pharma_quality_checks
ADD CONSTRAINT FOREIGN KEY(batch_id)
REFERENCES pharma_production_batches(batch_id);








-- ----------------------------  ***********************  ----------------------------------
-- ---------------------------- project question ans   -----------------------------------
-- ___________________________________________________________________________________________

-- Q1. Which batch has the highest failure rate, and what machine and operator produced it?
with fail_batch as
	(select batch_id , count(*) as fail_count
    from pharma_production_batches
    where quality_status = 'Fail'
    group by batch_id)

select p.batch_id, p.machine_id , p.operator_name , f.fail_count
from fail_batch as f
join pharma_production_batches as p
on f.batch_id = p.batch_id
order by fail_count desc limit 1;

-- which machin and operator has the highest failure rate ?
with fail_batch as
	(select machine_id, operator_name, count(*) as fail_count
	from pharma_production_batches 
	where quality_status = 'Fail'
    group by machine_id, operator_name)
    
select machine_id , operator_name , fail_count
from fail_batch
order by fail_count desc limit 5 ;

-- Q2. Identify batches where machine performance was poor AND product quality failed.
select * from pharma_production_batches
where quality_status = 'Fail'
and (Temperature_C > 80 or Pressure_bar < 1 or `Yield_%` < 75 )
order by `Yield_%` ; 

-- Q3. Which operators produce high-cost batches with low yield? (Window functions + numeric comparison)
with batch_stat as(
select operator_name, `Yield_%`, cost_per_batch,
	avg(cost_per_batch) over () as avg_cost ,
	avg(`Yield_%`) over () as avg_yield,
	rank() over(order by cost_per_batch desc) as cost_rank,
	rank() over(order by `Yield_%` asc ) as low_yield_rank
from pharma_production_batches)

select operator_name, `Yield_%` , cost_per_batch , cost_rank , low_yield_rank
from  batch_stat
where cost_per_batch > avg_cost and `Yield_%` < avg_yield
order by cost_per_batch desc, `Yield_%` asc;


-- Q4. Trend of failed batches over time — identify peak failure months. (CTE + date functions)
with fail_count as(
	select  month(production_date), year(production_date) , 
    count(*) as total_fail
	from pharma_production_batches
    where quality_status = 'Fail'
    group by  month(production_date) , year(production_date)),
    
		rank_fail as(
			select *, rank() over(order by total_fail desc) as fail_rank
            from fail_count)
    
    select * from rank_fail order by fail_rank;


-- Q5. Does the operating shift (A/B/C) impact quality failure percentage? (Group by shift + failure rate calc)
select shift , count(*) as total_batches,
sum(case when quality_status = 'Fail' then 1 else 0 end ) as fail_batches,
sum(case when quality_status = 'Fail' then 1 else 0 end) / count(*) * 100 as fail_percent
from pharma_production_batches
group by shift
order by fail_percent desc;


-- Q6. What machines have maximum downtime and how does it affect yield? (Join machine logs + production)

select m.machine_id , sum(m.downtime_hrs) as total_downtime,  avg(p.`Yield_%`) as avg_yield
from pharma_production_batches as p
join pharma_machine_logs as m
on p.machine_id = m.machine_id
group by m.machine_id
having  avg(p.`Yield_%`) < 70
order by total_downtime desc;  -- lost connection during this query


-- Q7. Which machine requires the most maintenance and is also linked to most product defects ?
with most_mentenance as(
		select m.machine_id , 
        sum(case when maintenance_required = 'YES' then 1 else 0 end) as need_mentenance ,
        count(q.defect_type) as defect_count
        from pharma_machine_logs as m
        join pharma_production_batches as p
        on m.machine_id = p.machine_id
        join pharma_quality_checks as q
        on p.batch_id = q.batch_id
        group by m.machine_id)
        
	select machine_id , need_mentenance , defect_count,
    rank() over(order by need_mentenance desc , defect_count desc) as machine_rank
    from most_mentenance
    order by machine_rank;

SET SESSION max_execution_time = 300000; -- 5 minutes

-- Q8. Rank machines by their productivity using runtime vs total batches produced. (Window function: RANK, DENSE_RANK)
with total_batches_produce as(
			select m.machine_id ,
            sum(m.Runtime_hrs) as total_runtime ,
            count(p.batch_id) as total_batch ,
            (count(p.batch_id)) / (sum(m.Runtime_hrs)) as productivity_hrs
		from pharma_machine_logs as m
        join pharma_production_batches as p
        on m.machine_id = p.machine_id
        group by machine_id)
        
	select 
        machine_id,
        total_runtime,
        total_batch,
        productivity_hrs,
        
        rank() over(order by productivity_hrs desc) as rank_productivity,
        dense_rank() over(order by productivity_hrs desc) as dense_productivity_hrs
        from total_batches_produce
        order by rank_productivity;

-- Q9. Identify the top 5 worst-performing machines using multiple KPIs (downtime, yield, defects).

with wors_performing as (
		select m.machine_id , 
				sum(m.downtime_hrs) as total_downtime,
				avg(p.`yield_%`) as avg_yield,
				sum(case when q.defect_type != 'NO DEFECT' then 1 else 0 end) as defect_count
			from pharma_machine_logs as m
			join pharma_production_batches as p
			  on m.machine_id = p.machine_id
			join pharma_quality_checks as q
			  on p.batch_id = q.batch_id 
			group by m.machine_id),

	ranked as (
		select machine_id, total_downtime , avg_yield , defect_count,
			   rank() over(order by total_downtime desc, -- more doentime means worse performance
			   avg_yield asc,      -- low yield means worse
			   defect_count desc   -- more defect count means worse
			   ) as worse_rank 
		from wors_performing )
        
select * from ranked 
where worse_rank <= 5
order by worse_rank;
                        
                   
-- Q10. Compare yield between machines with frequent maintenance vs machines with no maintenance.

with maintenance as ( 
				select m.machine_id, avg(p.`yield_%`) as average_yield,
                sum(case when m.maintenance_required = 'NO' then 1 else 0 end) no_need_maintenance,
                sum(case when m.maintenance_required = 'YES' then 1 else 0 end) need_maintenance
               from pharma_machine_logs as m
               join pharma_production_batches as p
                 on m.machine_id = p.machine_id
				group by m.machine_id)
                
		select machine_id ,average_yield, no_need_maintenance, need_maintenance
        from maintenance
        order by average_yield desc ;

                   
-- Q11. Which operator handled the highest number of failed batches ?

with number_of_fail_batch as(
			select operator_name, 
            count(case when quality_status = 'Fail' then 1 else 0 end) as handal_fail_batch
            from pharma_production_batches 
            group by  operator_name
            order by handal_fail_batch desc)
            
		select operator_name, handal_fail_batch 
        from number_of_fail_batch 
        limit 1;

-- Q12. Which QC inspector (Checked_By) flagged the most defects?

select q.checked_by , count(*) as defect_flagged
from pharma_quality_checks as q
join pharma_production_batches as p
 on q.batch_id = p.batch_id
 where p.quality_status ='FAIL'
 group by q.checked_by 
 order by defect_flagged desc;

-- Q13. Department-wise contribution to total production cost.
select e.department , sum(p.Cost_per_Batch) total_production_cost
from pharma_production_batches as p
join pharma_employees as e
on p.operator_name = e.Employee_Name
group by e.department
order by total_production_cost;


 -- Q14. Rank employees based on average yield of batches they operated.
 
select e.employee_name , avg(p.`yield_%`) as avg_yield ,
rank() over(order by avg(p.`yield_%`) desc) as ranks
from pharma_production_batches as p
join pharma_employees as e
on p.operator_name = e.Employee_Name
group by e.employee_name;


-- Q15. Compare salary vs performance (yield/defects). (Shows analytical thinking)
select e.Employee_Name , e.salary , avg(p.`Yield_%`) as avg_yield ,
sum(case when q.defect_type != 'NO DEFECT' THEN 1 ELSE 0 END) AS total_defect
from pharma_production_batches as p
join pharma_employees as e
on p.operator_name = e.Employee_Name
join pharma_quality_checks as q
on p.batch_id = q.batch_id
group by e.Employee_Name , e.salary
order by avg_yield ;

-- Q16. Which defect types occur the most, and in which products?
with defect_product as(
select p.product_name , q.defect_type, count(*)  AS defect_count,
rank() over(partition by p.product_name order by count(*) desc) as ranks
from pharma_quality_checks as q
join pharma_production_batches as p
on q.batch_id = p.batch_id
where q.Defect_Type != 'NO DEFECT'
group by q.Defect_Type , p.product_name)

select product_name , defect_type , defect_count, ranks
from defect_product
order by product_name , ranks;



-- Q17. What is the correlation between pH level, moisture %, and quality failures? (Complex grouping)

with correlation as (
select batch_id , ph , `moisture_%`,
case when defect_type = 'NO DEFECT' THEN 'PASS' ELSE 'failbatch' end as qu_status
from pharma_quality_checks as q
)

select avg(ph) as avg_ph , 
avg(`moisture_%`) , 
qu_status, 
count(*) total_stat
from correlation
group by  qu_status;


-- Q18. Identify products that consistently fail due to the same defect.
 select p.product_name , q.Defect_Type, count(*) as defect_count 
 from pharma_production_batches as p
 join pharma_quality_checks as q
   on p.batch_id = q.batch_id
where q.Defect_Type != 'no defect' and p.quality_status = 'Fail'
group by p.product_name , q.Defect_Type
order by defect_count desc;


WITH failure_cte AS (
    SELECT 
        p.Product_Name,
        q.Defect_Type,
        COUNT(*) AS cnt
    FROM pharma_production_batches p
    JOIN pharma_quality_checks q
        ON p.Batch_ID = q.Batch_ID
    WHERE p.Quality_Status = 'FAIL'
    GROUP BY p.Product_Name, q.Defect_Type
)
SELECT *
FROM failure_cte
WHERE cnt = (
    SELECT MAX(cnt)
    FROM failure_cte f2
    WHERE f2.Product_Name = failure_cte.Product_Name
    order by cnt desc
);



-- Q19. Are some QC inspectors stricter than others? (Compare rejection rate by inspector)

select q.checked_by , count(*) as total_check ,
sum(case when p.quality_status = 'Fail' then 1 else 0 end) as fail_status,
sum(case when p.quality_status = 'Fail' then 1 else 0 end) / count(*) * 100 as fail_percentage
from pharma_production_batches as p
join pharma_quality_checks as q
on p.batch_id = q.batch_id
group by q.checked_by
order by fail_percentage desc;




-- Q20. Find batches that passed QC even though machine logs show high downtime. (Critical thinking query)
                    
	WITH batch_downtime AS (
    SELECT p.batch_id, p.machine_id, p.production_date,
        MAX(m.downtime_hrs) AS max_downtime
    FROM pharma_production_batches p
    JOIN pharma_machine_logs m 
        ON p.machine_id = m.machine_id
       AND m.log_date = p.production_date    -- matching production day
    WHERE p.quality_status = 'PASS'
    GROUP BY 
        p.batch_id,
        p.machine_id,
        p.production_date
)
SELECT *
FROM batch_downtime
ORDER BY max_downtime DESC;

-- Q21. Which product has the highest cost per batch but lowest yield?
select product_name  , avg(`yield_%`) lowest_yield , avg(cost_per_batch) highest_cost
from pharma_production_batches 
group by batch_id 
order by highest_cost desc , lowest_yield asc;


-- Q22. Identify cost leakage: batches where cost is very high compared to yield (outliers). (Window function: PERCENTILE / MAD)

with cost_leakage as (
				   select batch_id , product_name , `yield_%` , cost_per_batch,
                   ntile(10) over( order by `yield_%` asc ) as avg_yield,
                   ntile(10) over(order by cost_per_batch desc) highest_batch
                   from pharma_production_batches 
                   )
                   
			select *
            from cost_leakage
            where avg_yield = 1 and highest_batch = 1
            order by cost_per_batch;
                   
                   
-- Q23. Department-wise cost distribution and which dept is most expensive.

select  E.Department , sum(E.salary) total_salary , sum(p.cost_per_batch) as cost_of_batch
from pharma_employees as e
join pharma_production_batches as p
on e.Employee_Name = p.operator_name
group by e.department
order by total_salary desc , cost_of_batch desc ;


-- Q24. Find the cheapest machine to operate per unit of output.
with machine_unit as(
select machine_id , sum(cost_per_batch) as total_cost , sum(`yield_%`) as total_yield,
round(sum(cost_per_batch) / nullif(sum(`yield_%`),0),2 ) as cost_per_output
from pharma_production_batches as p
group by machine_id )

select *,
rank() over(order by cost_per_output asc) as machine_rank
 from machine_unit
 order by machine_rank;
 
 
 -- Q25. Efficiency score per batch = Yield % / Cost (Create metric + rank batches)
 with efficiency as(
select batch_id, sum(`yield_%`) as total_yield , sum(cost_per_batch) as total_batch_cost ,
(sum(`yield_%`)) / (sum(cost_per_batch)) as efficiency_score
from pharma_production_batches 
group by batch_id )

select batch_id ,total_yield , total_batch_cost , efficiency_score ,
rank() over(order by efficiency_score desc) as good_effi
from efficiency
order by good_effi;

-- **Q26. Complete production Cycle Report:
-- Batch → Machine → Operator → QC Inspector → Quality Status** (Complex multi JOIN)
explain
SELECT p.Batch_ID,p.Production_Date,p.Machine_ID,p.Operator_Name,
q.Checked_By AS QC_Inspector,q.QC_Date,p.Quality_Status
FROM pharma_production_batches p
LEFT JOIN pharma_quality_checks q
    ON p.Batch_ID = q.Batch_ID
LEFT JOIN pharma_machine_logs m
    ON p.Machine_ID = m.Machine_ID
   AND DATE(m.Log_Date) = DATE(p.Production_Date)
ORDER BY p.Batch_ID;


-- **Q27. Predictive-style query: 
-- Based on past data, which machines are likely to require maintenance next month?** (Uses runtime patterns)

WITH monthly_stats AS (
    SELECT Machine_ID,DATE_FORMAT(Log_Date, '%Y-%m') AS month,
        SUM(Runtime_hrs) AS total_runtime,
        SUM(Downtime_hrs) AS total_downtime,
        SUM(CASE WHEN Maintenance_Required = 'Yes' THEN 1 ELSE 0 END) AS maint_count,
        
        ROW_NUMBER() OVER (PARTITION BY Machine_ID ORDER BY DATE_FORMAT(Log_Date, '%Y-%m')) AS rn
    FROM pharma_machine_logs
    GROUP BY Machine_ID, DATE_FORMAT(Log_Date, '%Y-%m')
),

runtime_trend AS (
    SELECT m1.Machine_ID, m1.month, m1.total_runtime, m1.total_downtime, m1.maint_count,
        -- previous month runtime
        m0.total_runtime AS prev_runtime,
        -- runtime growth
        (m1.total_runtime - m0.total_runtime) AS runtime_increase,
        -- previous month downtime
        m0.total_downtime AS prev_downtime,
        -- downtime growth
        (m1.total_downtime - m0.total_downtime) AS downtime_increase
    FROM monthly_stats m1
    LEFT JOIN monthly_stats m0
        ON m1.Machine_ID = m0.Machine_ID
        AND m1.rn = m0.rn + 1
)

SELECT 
    Machine_ID,
    month AS predicted_maintenance_month,
    total_runtime,
    total_downtime,
    runtime_increase,
    downtime_increase,
    maint_count,
    CASE 
        WHEN runtime_increase > 0 AND downtime_increase > 0 AND maint_count >= 2
        THEN 'HIGH RISK – Maintenance Likely Next Month'
        WHEN runtime_increase > 0 AND downtime_increase > 0
        THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
    END AS maintenance_prediction

FROM runtime_trend
ORDER BY Machine_ID, month;


-- Q28. Identify “at-risk” batches (low temperature + low pressure + low yield).

SELECT Batch_ID, Temperature_C, Pressure_bar, `Yield_%`,
    CASE WHEN Temperature_C < 70 AND Pressure_bar < 4 AND `Yield_%` < 70
            THEN 'HIGH RISK (Temp + Pressure + Yield)'
        WHEN (Temperature_C < 70 AND Pressure_bar < 4)
            THEN 'MEDIUM RISK (Temp + Pressure)'
        WHEN (Temperature_C < 70 AND `Yield_%` < 70)
            THEN 'MEDIUM RISK (Temp + Yield)'
        WHEN (Pressure_bar < 4 AND `Yield_%` < 70)
            THEN 'MEDIUM RISK (Pressure + Yield)'
        WHEN Temperature_C < 70
            THEN 'LOW RISK (Temperature)'
        WHEN Pressure_bar < 4
            THEN 'LOW RISK (Pressure)'
        WHEN `Yield_%` < 70
            THEN 'LOW RISK (Yield)'
        ELSE 'NO RISK'
    END AS Risk_Level
FROM pharma_production_batches;



