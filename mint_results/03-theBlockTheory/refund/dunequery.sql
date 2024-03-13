/*
Dune query link : https://dune.com/queries/3518978

*/

WITH onchainmint AS (
    SELECT
        tx_id,
        tx_signer
    FROM
        solana.instruction_calls
    WHERE
        block_slot > 253497079
        AND block_slot < 253866365
        AND executing_account = 'indxL6jiTVfJL48JFdRu7Bz4WKXBQX1otGgvnvpsaPE'
        AND tx_success
),
sentsol AS (
    SELECT
        tx_id
    FROM
        solana.account_activity
    WHERE
        block_slot > 253497079
        AND block_slot < 253866365
        AND address = '4rLG1485oqo7PEvCxtgs6BLzduFbCpmscpdxpWEZAhW8'
        AND token_mint_address IS NULL
        AND balance_change = 100000000
        AND tx_success
),
mint_attemps_table as (
    SELECT
        tx_signer as address,
        COUNT(DISTINCT tx_id) as attempts
    from
        onchainmint
    where
        tx_id in (
            select
                tx_id
            from
                sentsol
        )
    group by
        1
    order by
        2 desc
),
nft_mint_hashlist as (
    SELECT
        account_arguments [6] as mint_id
    FROM
        solana.instruction_calls
    where
        block_slot > 253370089
        AND block_slot < 253866365
        and is_inner = FALSE
        and executing_account = 'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR'
        and account_arguments [1] = '5iwvuMBshipp6iMFfrXnDJaykFFuYbUqgModBddu8LBu'
        and tx_success
),
nft_received_table as (
    SELECT
        to_owner as address,
        sum(amount) as mint_received
    FROM
        tokens_solana.transfers
    WHERE
        block_slot > 253370089
        AND block_slot < 253866365
        AND action = 'transfer'
        and from_owner = 'BE7rGpJZWtF19S848TCMijfxFNoZLMDKx6TAHoRTugxk'
        and amount = 1
        and token_mint_address in (
            select
                mint_id
            from
                nft_mint_hashlist
        )
    GROUP BY
        1
    order by
        2 desc
),
sent_refund_tx as (
    SELECT
        tx_id
    FROM
        solana.account_activity
    WHERE
        block_slot > 253550211
        AND block_slot < 253866365
        AND address = '121nSAGAUW81UTcyhHFByTJ9KFhWqviCE8vY3HHRCX7u'
        AND balance_change <= -10000000
        AND tx_success
),
refund_table as (
    SELECT
        address,
        sum(balance_change) / 100000000 as attempts_refunded
    FROM
        solana.account_activity
    WHERE
        block_slot > 253550211
        AND block_slot < 253866365
        AND address <> '121nSAGAUW81UTcyhHFByTJ9KFhWqviCE8vY3HHRCX7u'
        AND tx_id in (
            select
                tx_id
            from
                sent_refund_tx
        )
        AND tx_success
        AND balance_change >= 100000000
    GROUP BY
        1
    ORDER BY
        2 DESC
)
SELECT
    m.*,
    COALESCE(r.attempts_refunded, 0) as attempts_refunded,
    COALESCE(n.mint_received, 0) as mint_received,
    m.attempts - COALESCE(r.attempts_refunded, 0) - COALESCE(n.mint_received, 0) as refund_missing
from
    mint_attemps_table as m
    left JOIN refund_table as r on m.address = r.address
    left JOIN nft_received_table as n on m.address = n.address
order by
    mint_received desc