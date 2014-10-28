 --Create a table call transaction_customers_all that has all the visit history since the last 30 days of customers who purchased the previews day
 
 --Truncate the 86553600.step1 table
 --Save the results of this table at the 86553600.step1 table in BigQuery server
		SELECT 
			fullVisitorId, visitNumber, visitId, date, visitStartTime,
			totals.visits, totals.pageviews,
			totals.transactions, totals.transactionRevenue, totals.newVisits, trafficSource.referralPath,
			trafficSource.campaign, trafficSource.source, trafficSource.medium, trafficSource.keyword,
			trafficSource.adContent, hits.transaction.transactionId,
			lead(hits.transaction.transactionId) over (partition by fullVisitorId,visitId ORDER BY visitId,hits.transaction.transactionId) as flag_duplicate
        FROM
        (
				SELECT
					fullVisitorId, visitNumber, visitId, date, visitStartTime,
					totals.visits, totals.pageviews,
					totals.transactions, totals.transactionRevenue, totals.newVisits, trafficSource.referralPath,
					trafficSource.campaign, trafficSource.source, trafficSource.medium, trafficSource.keyword,
					trafficSource.adContent, hits.transaction.transactionId,
					lead(hits.transaction.transactionId) over (partition by fullVisitorId,visitId ORDER BY visitId,hits.transaction.transactionId) as flag_duplicate
				FROM  (TABLE_DATE_RANGE([86553600.ga_sessions_],DATE_ADD(CURRENT_TIMESTAMP(), -30, 'DAY'),DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')))
				GROUP EACH BY
					fullVisitorId, visitNumber, visitId, date, visitStartTime, totals.visits, totals.pageviews,
					totals.transactions, totals.transactionRevenue, totals.newVisits, trafficSource.referralPath,
					trafficSource.campaign, trafficSource.source, trafficSource.medium, trafficSource.keyword,
					trafficSource.adContent, hits.transaction.transactionId
				ORDER BY 
					fullVisitorId,visitId,hits.transaction.transactionId
        ) 
		WHERE 
            flag_duplicate IS NULL 
			--Get only visitors that made a transaction
            and fullVisitorId in (SELECT fullVisitorId from  (TABLE_DATE_RANGE([86553600.ga_sessions_],DATE_ADD(CURRENT_TIMESTAMP(), -2, 'DAY'),DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'))) where hits.transaction.transactionId is not NULL)
        ORDER BY 
		    fullVisitorId,visitId
