library(shroomDK)



api_key = ""
query <- { 
"
with nft_account_activity as (
  SELECT
    tx_to as address,
    mint,
    block_id,
    block_timestamp,
    tx_id
  from
    solana.core.fact_transfers
  where
    BLOCK_TIMESTAMP :: date >= '2021-08-01'
    AND BLOCK_TIMESTAMP :: date <= '2024-02-25'
    AND MINT in(
      SELECT
        MINT
      FROM
        solana.nft.dim_nft_metadata
    )
  order by
    BLOCK_TIMESTAMP
),
holder_nft as (
  SELECT
    mint,
    address,
    RANK() OVER (
      PARTITION BY mint,
      address
      ORDER BY
        block_id,
        tx_id
    ) AS owner_holding_mint_rank,
    block_id,
    block_timestamp,
    tx_id
  FROM
    nft_account_activity
  WHERE
    address not in (
      '1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix',
      '4zdNGgAtFsW1cQgHqkiWyRsxaAgxrSRRynnuunxzjxue'
    )
  order by
    block_id desc,
    tx_id
),
minter_table as (
  SELECT
    PURCHASER as ADDRESS,
    CASE
      WHEN MINT_CURRENCY = 'So11111111111111111111111111111111111111111' THEN MINT_PRICE
      ELSE 0.0
    END as SALES_AMOUNT,
    BLOCK_TIMESTAMP,
    TX_ID,
    'mint' as SIDE,
    MINT
  FROM
    solana.nft.fact_nft_mints
  WHERE
    IS_COMPRESSED = FALSE
    AND SUCCEEDED
  ORDER BY
    mint_price desc
),
buyer_table as(
  Select
    PURCHASER as ADDRESS,
    SALES_AMOUNT,
    BLOCK_TIMESTAMP,
    TX_ID,
    'buy' as SIDE,
    MINT
  FROM
    solana.nft.fact_nft_sales
  WHERE
    BLOCK_TIMESTAMP :: date >= '2023-01-01'
    AND BLOCK_TIMESTAMP :: date <= '2024-02-25'
    AND SUCCEEDED
    AND MINT in (
      SELECT
        mint
      from
        minter_table
    )
  ORDER BY
    BLOCK_TIMESTAMP DESC
),
seller_table as(
  Select
    SELLER as ADDRESS,
    SALES_AMOUNT,
    BLOCK_TIMESTAMP,
    TX_ID,
    'sell' as SIDE,
    MINT
  FROM
    solana.nft.fact_nft_sales
  WHERE
    BLOCK_TIMESTAMP :: date >= '2023-01-01'
    AND BLOCK_TIMESTAMP :: date <= '2024-02-25'
    AND SUCCEEDED
    AND MINT in (
      SELECT
        mint
      from
        minter_table
    )
  ORDER BY
    BLOCK_TIMESTAMP DESC
),
nft_trades_union as (
  SELECT
    *
  FROM
    buyer_table
  UNION
  ALL
  SELECT
    *
  FROM
    seller_table
  UNION
  ALL
  SELECT
    *
  FROM
    minter_table
),
nft_trades_temp as (
  select
    u.*,
    LAG(u.BLOCK_TIMESTAMP, 1) OVER (
      PARTITION BY u.ADDRESS
      ORDER BY
        u.BLOCK_TIMESTAMP
    ) AS PREV_TRADE_BLOCK_TIMESTAMP,
    h.block_timestamp as PREV_HOLD_BLOCK_TIMESTAMP
  from
    nft_trades_union as u
    left join holder_nft as h on u.mint = h.mint
    and u.ADDRESS = h.address
  WHERE
    h.owner_holding_mint_rank = 1
),
nft_trades_with_lasttrade as (
  SELECT
    ADDRESS,
    SALES_AMOUNT,
    BLOCK_TIMESTAMP,
    SIDE,
    DATEDIFF(
      MINUTE,
      PREV_TRADE_BLOCK_TIMESTAMP,
      BLOCK_TIMESTAMP
    ) AS MINUTES_SINCE_LAST_TRADE,
    DATEDIFF(
      MINUTE,
      PREV_HOLD_BLOCK_TIMESTAMP,
      BLOCK_TIMESTAMP
    ) AS MINUTES_SINCE_LAST_HOLD,
    MINT
  from
    nft_trades_temp
  WHERE
    PREV_TRADE_BLOCK_TIMESTAMP IS NOT NULL
  ORDER BY
    BLOCK_TIMESTAMP desc
)
SELECT
  ADDRESS,
  SUM(SALES_AMOUNT) as TOTAL_SOL_AMOUNT_TRADED,
  count(BLOCK_TIMESTAMP) as TOTAL_TRADE_COUNT,
  COUNT(DISTINCT MINT) as UNIQUE_NFT_TRADED,
  SUM(
    CASE
      WHEN SIDE = 'buy' then 1
      ELSE 0
    END
  ) as TOTAL_BUYS,
  SUM(
    CASE
      WHEN SIDE = 'sell' then 1
      ELSE 0
    END
  ) as TOTAL_SELLS,
  DATEDIFF(
    DAY,
    MIN(BLOCK_TIMESTAMP),
    MAX(BLOCK_TIMESTAMP)
  ) AS NB_DAYS_SINCE_FIRST_TRADE,
  AVG(MINUTES_SINCE_LAST_TRADE) as AVG_NB_MINUTES_BETWEEN_TRADES,
  AVG(
    CASE
      WHEN SIDE = 'sell' THEN MINUTES_SINCE_LAST_HOLD
      ELSE NULL
    END
  ) as AVG_NB_MINUTES_BETWEEN_HOLD_TRADE
from
  nft_trades_with_lasttrade
WHERE
  SIDE <> 'mint'
GROUP BY
  1
ORDER BY
  TOTAL_SOL_AMOUNT_TRADED DESC
LIMIT 900000
"
 }

# auto_paginate_query is a wrapper to all other steps. 
pull_data <- auto_paginate_query(
query = query,
api_key = api_key
)

print(nrow(pull_data))
write.csv(pull_data, "nft_traders_flipside.csv", row.names=FALSE)
