/*
    This model cleans the raw product data.
    1. It selects from the raw source.
    2. It extracts the 'rate' and 'count' from the messy JSON text column.
*/

with source as (
    -- "source" refers to the map we made in step 1
    select * from {{ source('ecommerce_source', 'PRODUCTS_RAW') }}
),

cleaned as (
    select
        id as product_id,
        title as product_name,
        price,
        description,
        category,
        image as image_url,
        
        -- 🔥 MAGIC: Parse that text string back into JSON and extract values
        -- We turn the text "{'rate': 3.9...}" into actual numbers
        try_parse_json(replace(rating, '''', '"')):rate::float as rating_stars,
        try_parse_json(replace(rating, '''', '"')):count::int as rating_count

    from source
)

select * from cleaned