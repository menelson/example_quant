with mint_burn AS (
    SELECT amount AS amount, 'rETH' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.rocket_july1_183days_vamm_evt_Mint
    UNION ALL
    SELECT -amount AS amount, 'rETH' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.rocket_july1_183days_vamm_evt_Burn
    UNION ALL
    SELECT amount AS amount, 'stETH' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.lido_july1_183days_vamm_evt_Mint
    UNION ALL
    SELECT  -amount AS amount, 'stETH' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.lido_july1_183days_vamm_evt_Burn
    UNION ALL
    SELECT amount AS amount, 'aETH_bORrow' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aave_ETH_bORrow_august22_131days_vamm_evt_Mint
    UNION ALL
    SELECT -amount AS amount, 'aETH_bORrow' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aave_ETH_bORrow_august22_131days_vamm_evt_Burn
    UNION ALL
    SELECT amount AS amount, 'cUSDT_bORrow' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.cUSDT_bORrow_august22_221days_vamm_evt_Mint
    UNION ALL
    SELECT -amount AS amount, 'cUSDT_bORrow' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.cUSDT_bORrow_august22_221days_vamm_evt_Burn
    UNION ALL
    SELECT amount AS amount, 'aUSDC_bORrow' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aUSDC_bORrow_august22_221days_vamm_evt_Mint
    UNION ALL
    SELECT -amount AS amount, 'aUSDC_bORrow' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aUSDC_bORrow_august22_221days_vamm_evt_Burn
    
    UNION ALL
    SELECT amount AS amount, 'aETH_bORrow_v2' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aETH_bORrow_september30_182days_vamm_evt_Mint
    UNION ALL
    SELECT -amount AS amount, 'aETH_bORrow_v2' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aETH_bORrow_september30_182days_vamm_evt_Burn
    UNION ALL
    SELECT amount AS amount, 'aDAI_v3' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aDAI_september30_92days_vamm_evt_Mint
    UNION ALL
    SELECT -amount AS amount, 'aDAI_v3' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aDAI_september30_92days_vamm_evt_Burn
    UNION ALL
    SELECT amount AS amount, 'aETH' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aETH_september30_92days_vamm_evt_Mint
    UNION ALL
    SELECT -amount AS amount, 'aETH' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aETH_september30_92days_vamm_evt_Burn
    UNION ALL
    SELECT amount AS amount, 'aUSDC_v3' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aUSDC_2_september30_92days_vamm_evt_Mint
    UNION ALL
    SELECT -amount AS amount, 'aUSDC_v3' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.aUSDC_2_september30_92days_vamm_evt_Burn
    UNION ALL
    SELECT amount AS amount, 'cDAI_v3' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.cDAI_2_september30_92days_vamm_evt_Mint
    UNION ALL
    SELECT -amount AS amount, 'cDAI_v3' as Token, tickLower AS lowerTick, tickUpper as upperTick, date_trunc('day', evt_block_time) as myDate
    FROM voltz_ethereum.cDAI_2_september30_92days_vamm_evt_Burn
), 

--- sum liquidity per tick range 
summ_date AS (
    SELECT 
        lowerTick,
        upperTick,
        Token,
        myDate,
        SUM(amount) AS amount
    FROM mint_burn
    GROUP BY myDate, Token, lowerTick, UpperTick
),

--- calculate cummulative liquidity (total liquidity ON the given day)
aggregated AS (
    SELECT 
        myDate,
        lowerTick,
        upperTick,
        Token,
        amount AS liq_day,
        SUM(amount) OVER (PARTITION BY Token,lowerTick, upperTick ORDER BY myDate ASC) AS liq_total
    FROM summ_date
    ORDER BY myDate, Token, lowerTick, UpperTick
),

--- separate tick ranges in ticks
spreaded_liq_ticks AS (
    SELECT
        myDate
        ,Token      
       ,explode(sequence(lowerTick, UpperTick, 60)) AS tick
       ,liq_day/abs((upperTick-lowerTick)/60) AS liq_day
       ,liq_total/abs((upperTick-lowerTick)/60) AS liq_total
    FROM aggregated
),

--- sum liquidity per tick
liq_ticks AS (
    SELECT
        myDate
        , Token      
       , tick
       , sum(liq_day) AS liq_day
       , sum(liq_total) AS liq_total
    FROM spreaded_liq_ticks
    GROUP BY myDate, Token, tick
),

--- get min AND max ticks of each day
max_min_ticks AS (
    SELECT
    min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    ,   'cUSDT_bORrow' AS Token
    FROM voltz_ethereum.cUSDT_bORrow_august22_221days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
    UNION ALL
    SELECT
    min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    ,   'aUSDC_bORrow' AS Token
    FROM voltz_ethereum.aUSDC_bORrow_august22_221days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
    UNION ALL
    SELECT 
    min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    ,   'rETH' AS Token
    FROM voltz_ethereum.rocket_july1_183days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
    UNION ALL
    SELECT 
    min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    ,   'stETH' AS Token
    FROM voltz_ethereum.lido_july1_183days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
    UNION ALL
    SELECT
    min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    ,   'aETH_bORrow_v2' AS Token
    FROM voltz_ethereum.aETH_bORrow_september30_182days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
    UNION ALL
    SELECT
    min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    ,   'aETH_bORrow' AS Token
    FROM voltz_ethereum.aave_ETH_bORrow_august22_131days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
    UNION ALL
    SELECT
    min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    ,   'aDAI_v3' AS Token
    FROM voltz_ethereum.aDAI_september30_92days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
    UNION ALL
    SELECT
    min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    ,   'aETH' AS Token
    FROM voltz_ethereum.aETH_september30_92days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
    UNION ALL
    SELECT min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    , 'cDAI_v3' AS Token
    FROM voltz_ethereum.cDAI_2_september30_92days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
    UNION ALL
    SELECT min(tick) AS minTick, max(tick) as maxTick, date_trunc('day', evt_block_time) as myDate
    ,   'aUSDC_v3' AS Token
    FROM voltz_ethereum.aUSDC_2_september30_92days_vamm_evt_VAMMPriceChange
    GROUP BY myDate
),

--- table with liquidity of each day combined with min AND max ticks of that day
sparce_priced_total_liq AS ( 
    SELECT
        a.myDate AS myDate,
        tick,
        a.Token,
        minTick,
        maxTick,
         CASE 
            WHEN liq_day is NULL then 0
            ELSE liq_day
       END AS liq_day
       , CASE 
            WHEN liq_total is NULL then 0
            ELSE liq_total
        END AS liq_total
    FROM liq_ticks a, max_min_ticks b
    where b.Token = a.Token AND a.myDate = b.myDate
),

--- liquidity traded in between min AND max ticks
sparce_priced_traded_liq AS ( 
    SELECT
        myDate,
        tick,
        Token,
        liq_day,
        liq_total,
        minTick,
        maxTick
    FROM sparce_priced_total_liq
    where tick >= minTick AND tick <=maxTick
),

--- sum liquidity of all ticks
summ_total_liq AS (
    SELECT 
    token, myDate, sum(liq_total) AS liq_total
    FROM sparce_priced_total_liq
    GROUP BY token, myDate
),

--- sum liquidity of ticks between min AND max
summ_traded_liq AS (
    SELECT 
    token, myDate, sum(liq_total) AS liq_total
    FROM sparce_priced_traded_liq
    GROUP BY token, myDate
),

result_tokens AS ( SELECT 
    a.Token,
    a.myDate AS myDate,
    a.liq_total/b.liq_total AS percentage_traded
FROM summ_traded_liq a
LEFT JOIN summ_total_liq b
ON a.Token = B.Token AND a.myDate = b.myDate
),

result_per_pool AS (
 SELECT 
    myDate
, CASE 
        WHEN Token = 'aETH_bORrow' then percentage_traded
        ELSE 0
    END AS percentage_traded_aETH_bORrow
, CASE 
        WHEN Token = 'aUSDC_bORrow' then percentage_traded
        ELSE 0
    END AS percentage_traded_aUSDC_bORrow
 , CASE 
        WHEN Token = 'aETH_bORrow_v2' then percentage_traded
        ELSE 0
    END AS percentage_traded_aETH_bORrow_v2
, CASE 
        WHEN Token = 'cUSDT_bORrow' then percentage_traded
        ELSE 0
    END AS percentage_traded_cUSDT_bORrow
 , CASE 
        WHEN Token = 'rETH' then percentage_traded
        ELSE 0
    END AS percentage_traded_rETH
, CASE 
        WHEN Token = 'stETH' then percentage_traded
        ELSE 0
    END AS percentage_traded_stETH
 , CASE 
        WHEN Token = 'aETH' then percentage_traded
        ELSE 0
    END AS percentage_traded_aETH
, CASE 
        WHEN Token = 'aUSDC_v3' then percentage_traded
        ELSE 0
    END AS percentage_traded_aUSDC
 , CASE 
        WHEN Token = 'cDAI_v3' then percentage_traded
        ELSE 0
    END AS percentage_traded_cDAI
, CASE 
        WHEN Token = 'aDAI_v3' then percentage_traded
        ELSE 0
    END AS percentage_traded_aDAI
FROM result_tokens
), 

result AS (
    SELECT
    sum(percentage_traded_aDAI) AS percentage_traded_aDAI,
    sum(percentage_traded_aETH_bORrow) AS percentage_traded_aETH_borrow,
    sum(percentage_traded_aETH) AS percentage_traded_aETH,
    sum(percentage_traded_aETH_bORrow_v2) AS percentage_traded_aETH_borrow_v2,
    sum(percentage_traded_aUSDC) AS percentage_traded_aUSDC,
    sum(percentage_traded_aUSDC_bORrow) AS percentage_traded_aUSDC_borrow,
    sum(percentage_traded_cDAI) AS percentage_traded_cDAI,
    sum(percentage_traded_cUSDT_bORrow) AS percentage_traded_cUSDT_borrow,
    sum(percentage_traded_rETH) AS percentage_traded_rETH,
    sum(percentage_traded_stETH) AS percentage_traded_stETH, ----
    myDate
    FROM result_per_pool
    GROUP BY myDate
    ORDER BY myDate
)

SELECT * FROM result