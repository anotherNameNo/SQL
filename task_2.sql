WITH cte AS(
	SELECT customer_id, contract_serial_number, loan_amount, issue_dt,
			EXTRACT(YEAR FROM first_issue_dt::date)::char(4) AS year_, 
			LPAD(EXTRACT(MONTH FROM first_issue_dt::date)::varchar(2), 2, '0') AS month_,
			FIRST_VALUE(loan_amount) OVER(PARTITION BY customer_id ORDER BY issue_dt) first_loan_amount
	FROM v_contract
	WHERE first_issue_dt BETWEEN '2019-01-01' AND '2020-01-01'
	),
	--
	--
	max_csn AS(
	SELECT year_, month_, MAX(contract_serial_number) AS csn_max
	FROM cte
	GROUP BY year_, month_
	),
	--
	--
	customer_max_csn AS(
	SELECT year_, month_, customer_id, contract_serial_number, loan_amount, first_loan_amount,
			ROUND((loan_amount::numeric /  first_loan_amount::numeric), 2) AS k_first_current
	FROM cte
	WHERE EXISTS (SELECT 1 FROM max_csn WHERE cte.contract_serial_number = max_csn.csn_max 
											AND cte.year_ = max_csn.year_ 
											AND cte.month_ = max_csn.month_)
	)
SELECT * FROM customer_max_csn
ORDER BY month_