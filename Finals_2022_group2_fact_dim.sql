
-- DATA WAREHOUSE

-- ############################################################################################################################################

-- Create table FACT_PRICING (pricing detail for every ticker recorded every 15 seconds for a change in price)
create table fact_pricing as
select 
    fv.price_id, 
    fv.ticker,
    from_unixtime(fv.window_start) as window_start,
    from_unixtime(fv.window_end) as window_end, 
    fv.usd_volume,
    fv.methodology, 
    fv.pricing_tier,
    from_unixtime(fv.effective_time) as effective_time,
    fv.usd_price
from full_window_price_v2 fv
group by 1,2,3,4,5,6,7,8,9 ;


-- Insert into table FACT_PRICING for regular updates
insert into fact_pricing
select * from 
(select fv.price_id, 
    fv.ticker,
    from_unixtime(fv.window_start) as window_start,
    from_unixtime(fv.window_end) as window_end, 
    fv.usd_volume,
    fv.methodology, 
    fv.pricing_tier,
    from_unixtime(fv.effective_time) as effective_time,
    fv.usd_price
from full_window_price_v2 fv
group by 1,2,3,4,5,6,7,8,9 
) z
left join fact_pricing fp on z.price_id = fp.price_id
where z.price_id is null ;

-- ############################################################################################################################################

-- Create table FACT_TRANSACTION (trading volume and price)
create table fact_transaction as
select
    c.ticker,
    from_unixtime(c.TStampTraded) as TStampTraded,
    c.currency,
    c.exchangeid as exchange_id,  
    c.usdPrice,
    c.usdsize
from conversion c
group by 1,2,3,4,5,6 ;

-- Insert into table FACT_TRANSACTION for regular updates
insert into fact_transaction
select * from 
(select
    c.ticker,
    from_unixtime(c.TStampTraded) as TStampTraded,
    c.currency,
    c.exchangeid as exchange_id,  
    c.usdPrice,
    c.usdsize
from conversion c
group by 1,2,3,4,5,6
) z
left join fact_transaction ft on z.ticker = ft.ticker and z.exchange_id = ft.exchange_id and z.TStampTraded = ft.TStampTraded
where z.ticker is null and z.exchange_id is null and z.TStampTraded is null ;

-- ############################################################################################################################################

-- Create table DIM_ASSET (asset details)
create table dim_asset as
select
    fv.ticker,
    fv.dar_identifier,
    fv.asset_name
from full_window_price_v2 fv    
group by 1,2,3 ;

-- Insert into table DIM_ASSET for regular updates
insert into dim_asset 
select * from
(select
    fv.ticker,
    fv.dar_identifier,
    fv.asset_name
from full_window_price_v2 fv    
group by 1,2,3 
) z
left join dim_asset a on z.ticker = a.ticker and z.dar_identifier = a.dar_identifier
where z.ticker is null and z.dar_identifier is null ;

-- ############################################################################################################################################

-- Create table DIM_PAIR (ticker and currency pair)
create table dim_pair as
select    
    c.currency,
    c.ticker,
    c.pair
from conversion c
group by 1,2,3 ;

-- Insert into table DIM_PAIR for regular updates
insert into dim_pair 
select * from
(select    
    c.currency,
    c.ticker,
    c.pair
from conversion c
group by 1,2,3
) z
left join dim_pair p on z.pair = a.pair 
where z.pair is null ;


