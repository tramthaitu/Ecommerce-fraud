CREATE TABLE shopee_bi (
    txn_time VARCHAR(255),
    txn_date VARCHAR(255),
    order_id VARCHAR(255),
    uid VARCHAR(255),
    shop_id VARCHAR(255),
    shop_owner_uid VARCHAR(255),
    gmv VARCHAR(255),
    rebate VARCHAR(255)
);

select *
from shopee_bi

--z-scores cho seller
WITH z_scores_shopper AS (
    SELECT 
        shop_id, shop_owner_uid,
        COUNT(order_id) AS transaction_cnt,
        ROUND(AVG(gmv::numeric),2) AS avg_value,
        ROUND((COUNT(order_id) - AVG(COUNT(order_id)) OVER ()) / STDDEV(COUNT(order_id)) OVER (),2) AS z_score_count,
        ROUND((AVG(gmv::numeric) - AVG(AVG(gmv::numeric)) OVER ()) / STDDEV(AVG(gmv::numeric)) OVER (),2) AS z_score_value,
        SUM(CASE WHEN gmv::numeric < 70000 AND rebate::numeric > 0 THEN 1 ELSE 0 END) AS low_value_rebate_cnt
    FROM shopee_bi
    GROUP BY shop_id, shop_owner_uid
)
SELECT 
    shop_id, shop_owner_uid,
    transaction_cnt, 
    avg_value,
    z_score_count, 
    z_score_value,
    low_value_rebate_cnt
FROM z_scores_shopper
WHERE ABS(z_score_count) >= 3  -- Số lượng giao dịch bất thường
   OR ABS(z_score_value) <= -3 -- Giá trị đơn hàng trung bình quá nhỏ
   OR low_value_rebate_cnt > 0  -- Có giao dịch giá trị nhỏ nhưng vẫn có rebate
ORDER BY transaction_cnt DESC;

-- z-scores cho buyer
WITH z_scores_users AS (
    SELECT 
        uid,
        COUNT(order_id) AS transaction_cnt,
        ROUND(AVG(gmv::numeric),2) AS avg_order_value,
        ROUND((COUNT(order_id) - AVG(COUNT(order_id)) OVER ()) / STDDEV(COUNT(order_id)) OVER (),2) AS z_score_transaction,
        ROUND((AVG(gmv::numeric) - AVG(AVG(gmv::numeric)) OVER ()) / STDDEV(AVG(gmv::numeric)) OVER (),2) AS z_score_value,
        SUM(CASE WHEN gmv::numeric < 70000 AND rebate::numeric > 0 THEN 1 ELSE 0 END) AS low_value_rebate_cnt
    FROM shopee_bi
    WHERE rebate IS NOT NULL
    GROUP BY uid
)
SELECT 
    uid, 
    transaction_cnt, 
    avg_order_value,
    z_score_transaction, 
    z_score_value, 
    low_value_rebate_cnt
FROM z_scores_users
WHERE ABS(z_score_transaction) >= 3  -- Giao dịch quá nhiều so với trung bình
   OR ABS(z_score_value) <= -3       -- Giá trị đơn hàng quá thấp
   OR low_value_rebate_cnt > 0       -- Có đơn hàng thấp hơn 70.000 vẫn dùng mã giảm giá
ORDER BY transaction_cnt DESC;

-- Người mua gian lận
SELECT
	uid,shop_owner_uid,
    --txn_time :: timestamp ,
    --Lead(txn_time) OVER (PARTITION BY uid ORDER BY txn_time):: timestamp AS next_orders,
	Lead(txn_time) OVER (PARTITION BY uid ORDER BY txn_time):: timestamp -txn_time :: timestamp  AS Period,
	Count(order_id) over (Partition by shop_owner_uid) as transactions,
	gmv,rebate
FROM shopee_bi
group by uid,order_id, txn_time,gmv,rebate,shop_owner_uid
having uid::numeric in (1026737, 100605978, 100205391) -- 1026737, 100605978, 100205391
order by uid, transactions

--Người bán gian lận
select shop_owner_uid, uid,
--txn_time,
--Lead(txn_time) OVER (PARTITION BY uid ORDER BY txn_time):: timestamp,
Lead(txn_time) OVER (PARTITION BY uid ORDER BY txn_time):: timestamp -txn_time :: timestamp  AS Period,
Count(order_id) over (Partition by uid) as transactions,
gmv, rebate
From shopee_bi
group by shop_owner_uid,uid, gmv, rebate, txn_time, order_id
having shop_owner_uid = '103280547'
order by uid, transactions


