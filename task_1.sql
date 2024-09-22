DROP TABLE IF EXISTS v_contract;
--
--
CREATE TABLE IF NOT EXISTS v_contract 
AS (
WITH pre_contract AS (
    SELECT  DISTINCT contract_id, contract_code, customer_id, subdivision_id, issue_dt, 
    		renewal_contract_id IS NOT NULL AND renewal_contract_id <> '' AS is_renewal
    FROM test_data_contract
    WHERE NOT marker_delete
    --ORDER BY customer_id, issue_dt, contract_id
    ),
    --
    --
    contract_customer AS(
    SELECT DISTINCT customer_id, contract_id FROM test_data_contract
    ),
    --
    --
    contract AS (
    SELECT *,
    		ROW_NUMBER() OVER(PARTITION BY customer_id 
    							ORDER BY issue_dt) AS contract_serial_number,
    		SUM(CAST(NOT is_renewal AS integer)) OVER(PARTITION BY customer_id 
    													ORDER BY issue_dt) AS contract_renewal_serial_number,
    		MIN(issue_dt) OVER(PARTITION BY customer_id) AS first_issue_dt
    FROM pre_contract
    ),
    --
    --
    pre_contract_conditions AS(
    SELECT  DISTINCT condition_id, condition_dt, contract_id, operation_type, 
    		condition_type, condition_start_dt, condition_end_dt, days,
    		CASE 
	    		WHEN condition_type = 'Продление' THEN 1 
	    		ELSE 0 
	    	END AS pre_prolong_count,
	    	FIRST_VALUE(condition_end_dt) OVER(PARTITION BY contract_id 
    											ORDER BY  condition_dt ) AS plan_dt,
    		FIRST_VALUE(days) OVER(PARTITION BY contract_id 
    											ORDER BY  condition_dt ) AS loan_term
    FROM test_data_contract_conditions
    WHERE NOT marker_delete AND conducted
    --ORDER BY contract_id, condition_dt, condition_id
    ),
    --
    --
    prolong_count_plan_dt_loan_term AS(
    SELECT DISTINCT contract_id, plan_dt, loan_term, prolong_count
    FROM
	    (SELECT *,
	    		SUM(pre_prolong_count) OVER(PARTITION BY contract_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prolong_count
	    FROM (SELECT DISTINCT contract_id,  condition_id, pre_prolong_count, plan_dt, loan_term FROM pre_contract_conditions)
	    ) AS pcpdlt
    ),
    --
    --
    payment_plan AS (
    SELECT customer_id, contract_id, 
    		MAX(payment_dt) AS last_plan_dt,
    		(COUNT(payment_dt) > 1) AS is_installment,
    		SUM(loan_amount) AS loan_amount,
    		SUM(SUM(loan_amount)) OVER(PARTITION BY customer_id ORDER  BY MAX(payment_dt)) AS total_loan_amount,
    		MIN(SUM(loan_amount)) OVER(PARTITION BY customer_id ORDER  BY MAX(payment_dt)) AS min_loan_amount,
    		MAX(SUM(loan_amount)) OVER(PARTITION BY customer_id ORDER  BY MAX(payment_dt)) AS max_loan_amount
    FROM (SELECT DISTINCT contract_id, condition_id FROM pre_contract_conditions) AS ppp
    LEFT JOIN (SELECT DISTINCT * FROM test_data_contract_conditions_payment_plan) AS ccp USING (condition_id)
    LEFT JOIN contract_customer USING (contract_id)
    GROUP BY customer_id, contract_id
    --ORDER BY customer_id, MAX(payment_dt)
    ),
    --
    --
    pre_contract_status AS(
    SELECT *,
    		CASE 
	    		WHEN (LAST_VALUE(status_type) OVER(PARTITION BY contract_id 
	    											ORDER BY  status_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
    			IN ('Переоформлен', 'Закрыт', 'Договор закрыт с переплатой')) THEN TRUE 
    			ELSE FALSE 
    		END AS is_closed,
    		LAST_VALUE(status_dt) OVER(PARTITION BY contract_id 
    									ORDER BY  status_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_status_dt,
    		FIRST_VALUE(status_dt) OVER(PARTITION BY contract_id 
    									ORDER BY  status_dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_status_dt
    FROM (SELECT DISTINCT * FROM test_data_contract_status) AS pcs
    LEFT JOIN contract_customer USING(contract_id)
    --ORDER BY customer_id, status_dt, contract_id
    ),
    --
    --
    contract_status AS(
    SELECT  customer_id, contract_id, is_closed,
    		CASE 
	    		WHEN is_closed THEN (last_status_dt::date - first_status_dt::date) 
    			ELSE NULL 
    		END AS usage_days,
    		CASE 
	    		WHEN COALESCE(LEAD(contract_id) OVER(PARTITION BY customer_id 
	    												ORDER BY  first_status_dt), '') <> '' THEN TRUE 
	    		ELSE FALSE 
	    	END AS has_next,
    		first_status_dt::date - (LAG(last_status_dt) OVER(PARTITION BY customer_id 
    															ORDER BY  last_status_dt))::date AS delay_days,
    		CASE
    			WHEN is_closed THEN last_status_dt::date
    			ELSE NULL 
    		END AS close_dt
    FROM (SELECT DISTINCT customer_id, contract_id, is_closed, first_status_dt, last_status_dt FROM pre_contract_status) AS cs
    --ORDER BY customer_id, last_status_dt, contract_id
    ),
    --
    --
    contract_conditions AS(
    SELECT contract_id, condition_id
    FROM pre_contract_conditions
    WHERE operation_type='ЗаключениеДоговора'
    GROUP BY contract_id, condition_id
    ),
    --
    --
    all_table AS(
    SELECT *,
    		close_dt::date - last_plan_dt::date AS dev_days,
    		MIN(loan_term) OVER(PARTITION BY customer_id ORDER  BY last_plan_dt) AS min_loan_term, 
    		MAX(loan_term) OVER(PARTITION BY customer_id ORDER  BY last_plan_dt) AS max_loan_term
    FROM contract_customer
    LEFT JOIN contract_conditions USING(contract_id)
    LEFT JOIN contract USING(contract_id, customer_id)
    LEFT JOIN prolong_count_plan_dt_loan_term USING(contract_id)
    LEFT JOIN payment_plan USING(contract_id, customer_id)
    LEFT JOIN contract_status USING(contract_id, customer_id)
    ORDER BY customer_id, issue_dt
    )
--
--
SELECT * FROM all_table);
ALTER TABLE v_contract ADD PRIMARY KEY (contract_id);
CREATE INDEX ix_v_contract_customer_id ON v_contract(customer_id);
CREATE INDEX ix_v_contract_contract_id ON v_contract(contract_id);
CREATE INDEX ix_v_contract_condition ON v_contract(condition_id);
COMMENT ON TABLE v_contract IS 'Итоговая витрина';
--
--
COMMENT ON COLUMN v_contract.contract_id IS 'ID контракта';
COMMENT ON COLUMN v_contract.contract_code IS 'Код контракта';
COMMENT ON COLUMN v_contract.customer_id IS 'ID клиента';
COMMENT ON COLUMN v_contract.condition_id IS 'ID документа о заключении контракта';
COMMENT ON COLUMN v_contract.subdivision_id IS 'ID подразделения, заключившего контракт';
COMMENT ON COLUMN v_contract.contract_serial_number IS 'Порядковый номер контракта у клиента';
COMMENT ON COLUMN v_contract.contract_renewal_serial_number IS 'Порядковый номер контракта у клиента, без учета переоформления';
COMMENT ON COLUMN v_contract.is_renewal IS 'Признак переоформления контракта';
COMMENT ON COLUMN v_contract.is_installment IS 'Признак долгосрочного контракта';
COMMENT ON COLUMN v_contract.prolong_count IS 'Количество продлений контракта';
COMMENT ON COLUMN v_contract.first_issue_dt IS 'Дата первого контракта у клиента';
COMMENT ON COLUMN v_contract.issue_dt IS 'Дата выдачи займа';
COMMENT ON COLUMN v_contract.plan_dt IS 'Дата планового погашения займа';
COMMENT ON COLUMN v_contract.close_dt IS 'Дата фактического погашения займа (дата закрытия)';
COMMENT ON COLUMN v_contract.last_plan_dt IS 'Дата планового погашения займа с учётом продлений';
COMMENT ON COLUMN v_contract.loan_amount IS 'Сумма займа';
COMMENT ON COLUMN v_contract.total_loan_amount IS 'Сумма всех предыдущих займов';
COMMENT ON COLUMN v_contract.min_loan_amount IS 'Минимальная сумма предыдущих займов';
COMMENT ON COLUMN v_contract.max_loan_amount IS 'Максимальная сумма предыдущих займов';
COMMENT ON COLUMN v_contract.loan_term IS 'Срок займа в днях';
COMMENT ON COLUMN v_contract.min_loan_term IS 'Минимальный срок предыдущих займов';
COMMENT ON COLUMN v_contract.max_loan_term IS 'Максимальный срок предыдущих займов';
COMMENT ON COLUMN v_contract.is_closed IS 'Контракт закрыт';
COMMENT ON COLUMN v_contract.usage_days IS 'Количество дней фактического использования займа (для закрытых)';
COMMENT ON COLUMN v_contract.dev_days IS 'Разница между датой закрытия и последней датой планового погашения (для закрытых)';
COMMENT ON COLUMN v_contract.delay_days IS 'Количество дней с даты закрытия предыдущего займа у клиента и датой открытия текущего';
COMMENT ON COLUMN v_contract.has_next IS 'Признак наличия следующего контракта у клиента';
--
--
--SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'v_contract' ORDER BY ordinal_position;
--SELECT conname, contype FROM pg_constraint WHERE conrelid = 'v_contract'::regclass;
