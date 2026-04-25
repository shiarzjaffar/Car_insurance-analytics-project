create database InsuranceActuarialDB;
GO

USE InsuranceActuarialDB;
GO

CREATE TABLE policy_frequency_stage (
    policy_id VARCHAR(50),
    claim_nb VARCHAR(50),
    exposure VARCHAR(50),
    car_power VARCHAR(50),
    car_age VARCHAR(50),
    driver_age VARCHAR(50),
    bonus_malus VARCHAR(50),
    brand VARCHAR(50),
    gas VARCHAR(50),
    area VARCHAR(50),
    density VARCHAR(50),
    region VARCHAR(50)
);

CREATE TABLE claim_severity (
    policyid BIGINT,
    claimamount FLOAT
);

BULK INSERT policy_frequency_stage
FROM 'D:\acturial Analyst project\archive (1)\freMTPL2freq.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

select * from policy_frequency_stage

select count(*) from policy_frequency_stage 

CREATE TABLE policy_frequency (
    policy_id bigint,
    claim_nb int,
    exposure float,
    car_power int,
    car_age int,
    driver_age int,
    bonus_malus int,
    brand VARCHAR(10),
    gas VARCHAR(20),
    area VARCHAR(2),
    density int,
    region VARCHAR(50)
);

insert into policy_frequency
select 
TRY_CAST(policy_id as bigint),
TRY_CAST(claim_nb as int),
TRY_CAST(exposure as float),
TRY_CAST(car_power as int),
TRY_CAST(car_age as int),
TRY_CAST(driver_age as int),
TRY_CAST(bonus_malus as int),
TRY_CAST(brand as VARCHAR(10)),
TRY_CAST(gas as VARCHAR(20)),
TRY_CAST(area as VARCHAR(2)),
TRY_CAST(density as int),
TRY_CAST(region as VARCHAR(50))
from policy_frequency_stage

select * from policy_frequency where policy_id is null
select * from policy_frequency_stage where policy_id = '1.00E+05'

BULK INSERT claim_severity
FROM 'D:\acturial Analyst project\archive (1)\freMTPL2sev.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

select count(policyid) from claim_severity
select count(policy_id) from policy_frequency

CREATE TABLE policy_frequency_final (
    policy_id bigint primary key not null,
    claim_nb int,
    exposure float,
    car_power int,
    car_age int,
    driver_age int,
    bonus_malus int,
    brand VARCHAR(10),
    gas VARCHAR(20),
    area VARCHAR(2),
    density int,
    region VARCHAR(50)
);

insert into policy_frequency_final
select 
case
when policy_id is null then 100000
when policy_id is not null then policy_id
end as policy_id,
claim_nb,
exposure,
car_power,
car_age,
driver_age,
bonus_malus,
trim(brand),
trim(gas),
trim(area),
density,
Trim(region)
from policy_frequency order by policy_id asc

select policy_id, count(*) as dup_count from policy_frequency group by policy_id having count(*) > 1 

select count(policy_id) from policy_frequency
select count(distinct policy_id) from policy_frequency

select distinct brand from policy_frequency

select * from claim_severity order by policyid asc

create view VW_policy as
select 
F.policy_id,
F.claim_nb,
F.exposure,
F.car_power,
F.car_age,
F.driver_age,
F.bonus_malus,
F.brand,
F.gas,
F.area,
F.density,
F.region,
C.claimamount
from policy_frequency_final F 
left join claim_severity C
on F.policy_id = C.policyid;

--  Claim Frequency

select 
avg(cast(claim_nb as FLOAT)) as claim_avg
from VW_policy

--Claim Severity

select 
avg(CAST( claimamount as float )) as claimamount_avg
from VW_policy
where claimamount is not null

-- Claim Severity

select(
select avg(cast(claim_nb as FLOAT)) as claim_avg from VW_policy) 
*
(select 
avg(CAST( claimamount as float )) as claimamount_avg
from VW_policy
) as expected_loss_per_policy

-- Risk Segmentation (Driver Age)

select 
case 
when driver_age < 25 then 'Young'
when driver_age between 25 and 60 then 'Adult'
else 'Senior' end as driver_age_group,
count(*) as total_policies,
sum(claim_nb) as total_claim,
AVG(cast(claim_nb as float)) as avg_claim,
avg(cast(claimamount as float)) as avg_claim_amount
from VW_policy
group by 
case 
when driver_age < 25 then 'Young'
when driver_age between 25 and 60 then 'Adult'
else 'Senior' end
order by avg_claim desc

-- Risk Segmentation by Vehicle Power

select * from VW_policy

select car_power,
count(*) as total_policies,
AVG(CAST(claim_nb as float)) as claim_freq,
AVG(CAST(claimamount as float)) as claim_amount_avg
from VW_policy
group by car_power
order by claim_amount_avg desc

select distinct car_age, count(*) as number_of_cars from VW_policy group by car_age order by car_age desc

select
case
when car_age < 25 then 'modern'
when car_age between 25  and 75 then 'Classic'
else 'Vintage' end,
sum(claim_nb) as total_policies,
avg(cast(claim_nb as float)) as claim_freq,
avg(cast(claimamount as float)) as claim_severity
from VW_policy
group by 
case
when car_age < 25 then 'modern'
when car_age between 25  and 75 then 'Classic'
else 'Vintage' end
order by claim_severity desc

-- Suggested premium

select 
policy_id,
driver_age,
claim_nb * claimamount as expected_loss,
case
when driver_age < 25 then  claim_nb * claimamount *1.5
else claim_nb * claimamount * 1.2 end as suggested_premius
from VW_policy

CREATE VIEW vw_dashboard_actuarial AS
SELECT
    -- Risk Segmentation
    CASE 
        WHEN driver_age < 25 THEN 'Young'
        WHEN driver_age BETWEEN 25 AND 60 THEN 'Adult'
        ELSE 'Senior'
    END AS driver_age_group,
    car_power,
    region,

    -- Aggregated Metrics
    COUNT(*) AS total_policies,
    SUM(claim_nb) AS total_claims,
    AVG(CAST(claim_nb AS FLOAT)) AS avg_claim_frequency,
    AVG(CAST(claimamount AS FLOAT)) AS avg_claim_severity,
    -- Expected loss per policy
    AVG(CAST(claim_nb AS FLOAT) * CAST(claimamount AS FLOAT)) AS expected_loss,
    -- Suggested premium with 20% loading factor
    AVG(CAST(claim_nb AS FLOAT) * CAST(claimamount AS FLOAT) * 1.2) AS suggested_premium
FROM VW_policy
GROUP BY
    CASE 
        WHEN driver_age < 25 THEN 'Young'
        WHEN driver_age BETWEEN 25 AND 60 THEN 'Adult'
        ELSE 'Senior'
    END,
    car_power,
    region;
