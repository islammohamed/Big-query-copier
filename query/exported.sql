--Import the results of this query to our database at the test.import_table		
SELECT
c.country as country,
a.fullVisitorId as visitor_id, 
a.visitNumber as visit_number, 
a.visitId as visit_id, 
a.date as date, 
a.visitStartTime as visit_start_time,
a.totals_visits as total_visits,
a.totals_pageviews as total_pageviews, 
a.totals_transactions as total_transactions, 
a.totals_transactionRevenue as total_transaction_revenue, 
a.totals_newVisits as total_new_visits, 
a.trafficSource_referralPath as referral_path, 
a.trafficSource_campaign as campaign,
a.trafficSource_source as source, 
a.trafficSource_medium as medium, 
a.trafficSource_keyword as keyword, 
a.trafficSource_adContent as adContent, 
a.hits_transaction_transactionId as transaction_id  
FROM
  [all.foodpanda_aggregatted_table] a 
JOIN  
       (SELECT 
        fullVisitorId, MAX(visitId) AS last_visitId_with_transaction, MAX(visitNumber) AS last_visit_number_with_transaction, MAX(date) as last_date_with_transaction 
       FROM
        (TABLE_DATE_RANGE([86553600.ga_sessions_],DATE_ADD(CURRENT_TIMESTAMP(), -2, 'DAY'),DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')))
       WHERE 
        hits.transaction.transactionId IS NOT NULL
       GROUP BY 
        fullVisitorId
       ORDER BY 
        fullVisitorId) b 
ON a.fullVisitorId=b.fullVisitorId
JOIN
      (SELECT  fullVisitorID, customDimensions.value AS country FROM (TABLE_DATE_RANGE([86553600.ga_sessions_],DATE_ADD(CURRENT_TIMESTAMP(), -2, 'DAY'),DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')))
       WHERE customDimensions.index=3 GROUP BY fullVisitorId,country) c
ON a.fullVisitorId=C.fullVisitorId
WHERE 
  a.visitId<=b.last_visitId_with_transaction
ORDER BY 
  c.country,a.fullVisitorId, a.visitId
  


  
  
  

