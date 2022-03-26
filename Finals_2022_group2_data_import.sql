--------------------------------------
-- 1. create the relevant data base -- 
--------------------------------------

CREATE DATABASE DM_project;
 


--------------------------------------
-- 2. select the relevant database ---
--------------------------------------

USE `DM_project`;

--------------------------------------
-- 3. loading full_window_price_v2 --- 
--------------------------------------


-- 3.1 creating the table 
CREATE TABLE `full_window_price_v2` (
`ticker` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
`methodology` varchar(12) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
`window_start` bigint(20) NOT NULL,
`window_end` bigint(20) NOT NULL,
`usd_price` double NOT NULL,
`usd_volume` double NOT NULL,
`effective_time` bigint(20) NOT NULL,
`price_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
`dar_identifier` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
`pricing_tier` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
`asset_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
UNIQUE KEY `ckey_ticr_method_eff_time_u2` (`ticker`,`methodology`,`effective_time`) USING HASH,
KEY `idx_eff_time_12` (`effective_time`) USING CLUSTERED COLUMNSTORE,
SHARD KEY `idx_eff_method_12` (`effective_time`,`methodology`)
) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
 

-- 3.2 creating the pipeline 
CREATE AGGREGATOR PIPELINE `full_window_price_v2`
AS LOAD DATA S3 's3://pricing-worker/dar-prod/Historic_Price_File/2022/2'
CONFIG '{\"region\": \"us-east-1\"}'
CREDENTIALS '{"aws_access_key_id": "AKIAR42WX226QWI2QWTO", "aws_secret_access_key": "M64VGvP7ICVhCo7P/KzO8h+YvlxLsHC+eRzjK4Xt"}'
BATCH_INTERVAL 10000
MAX_PARTITIONS_PER_BATCH 1
DISABLE OUT_OF_ORDER OPTIMIZATION
REPLACE
INTO TABLE `full_window_price_v2`
FIELDS TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n' STARTING BY ''
(
   `full_window_price_v2`.`ticker`,
   `full_window_price_v2`.`methodology`,
   `full_window_price_v2`.`window_start`,
   `full_window_price_v2`.`window_end`,
   `full_window_price_v2`.`usd_price`,
   `full_window_price_v2`.`usd_volume`,
   `full_window_price_v2`.`effective_time`,
   `full_window_price_v2`.`price_id`,
   `full_window_price_v2`.`dar_identifier`,
   `full_window_price_v2`.`pricing_tier`,
   `full_window_price_v2`.`asset_name`
);

-- 3.3 starting the pipeline 
START PIPELINE `full_window_price_v2` FOREGROUND;
 

--------------------------------------
-- 4. loading conversion ------------- 
--------------------------------------

-- 4.1 crating the table
CREATE TABLE `conversion` (
`ticker` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
`exchangeId` bigint(20) NOT NULL,
`pair` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
`currency` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
`price` NUMERIC NOT NULL,
`size` bigint(20) NOT NULL,
`rate` NUMERIC NOT NULL,
`usdPrice` NUMERIC NOT NULL,
`usdSize` NUMERIC NOT NULL,
`TStampTraded` NUMERIC NOT NULL,
UNIQUE KEY `ckey_ticr_exchange_tradeTime` (`ticker`,`exchangeId`,`TStampTraded`) USING HASH,
KEY `TStampTraded` (`TStampTraded`) USING CLUSTERED COLUMNSTORE,
SHARD KEY `idx_ticr_exchange_tradeTime` (`ticker`,`exchangeId`,`TStampTraded`)
) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
 

-- 4.2 creating the pipeline
CREATE AGGREGATOR PIPELINE `conversion`
AS LOAD DATA S3 's3://pricing-worker/dar-prod/conversion/2022/2'
CONFIG '{\"region\": \"us-east-1\"}'
CREDENTIALS '{"aws_access_key_id": "AKIAR42WX226QWI2QWTO", "aws_secret_access_key": "M64VGvP7ICVhCo7P/KzO8h+YvlxLsHC+eRzjK4Xt"}'
BATCH_INTERVAL 10000
MAX_PARTITIONS_PER_BATCH 1
DISABLE OUT_OF_ORDER OPTIMIZATION
REPLACE
INTO TABLE `conversion`
 
FIELDS TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n' STARTING BY ''
(
   `conversion`.`ticker`,
   `conversion`.`exchangeId`,
   `conversion`.`pair`,
   `conversion`.`currency`,
   `conversion`.`price`,
   `conversion`.`size`,
   `conversion`.`rate`,
   `conversion`.`usdPrice`,
   `conversion`.`usdSize`,
   `conversion`.`TStampTraded`
);


-- 4.3 starting the pipeline
START PIPELINE `conversion` FOREGROUND;








