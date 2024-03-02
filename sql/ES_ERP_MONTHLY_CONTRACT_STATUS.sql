create or replace view ES_ERP_MONTHLY_CONTRACT_STATUS(
	MONTH_ID,
	CONTRACT_NUMBER,
	STATUS,
	CONTRACT_STATUS,
	EOM,
	LAST_DAY_BY_MONTH
) as (

with base as (
SELECT
contract.*
,((year(contract.cancelon) * 100) + month(contract.cancelon)) AS CHURN_MONTH_ID
FROM
PURCHASING.PUBLISHED_PROD.PHOENIX_XRBCONTRHDR contract
--WHERE contract.maklsyncdate = (SELECT max(maklsyncdate) FROM   PURCHASING.TRANSFORMED_PROD.PHOENIX_XRBCONTRHDR)
--where contract.contractcd='CT00005988'
)

,base1 as (
select DISTINCT YEAR(maklsyncdate)*100+MONTH(maklsyncdate) as MONTH_ID
,contractcd AS CONTRACT_NUMBER
,status
,case when status='O' THEN 'OPEN' WHEN status='X' THEN 'CANCELLED' WHEN status='H' THEN 'HOLD' WHEN status='C' THEN 'CLOSED' END AS CONTRACT_STATUS
,LAST_DAY(maklsyncdate,'month') as EOM
,row_number() over (partition by contractcd,year(maklsyncdate),month(maklsyncdate) order by day(maklsyncdate) desc) as Last_day_by_month
from base
where Year(maklsyncdate)>=2020
)

select * from base1
where Last_day_by_month=1
--and CONTRACT_STATUS='HOLD'
  );