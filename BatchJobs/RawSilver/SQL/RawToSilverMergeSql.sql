MERGE INTO s3tablesbucket.example_namespace.orders AS target
    USING (
        SELECT
            invoiceid,
            itemid,
            category,
            price,
            quantity,
            orderdate,
            destinationstate,
            shippingtype,
            referral
        FROM (
                 SELECT *,
                        ROW_NUMBER() OVER (
                   PARTITION BY invoiceid, itemid
                   ORDER BY replicadmstimestamp DESC
               ) AS row_num
                 FROM source_table
             ) AS deduped_source
        WHERE row_num = 1
    ) AS source
    ON target.invoiceid = source.invoiceid AND target.itemid = source.itemid
    WHEN MATCHED THEN
        UPDATE SET
            target.category = source.category,
            target.price = source.price,
            target.quantity = source.quantity,
            target.orderdate = source.orderdate,
            target.destinationstate = source.destinationstate,
            target.shippingtype = source.shippingtype,
            target.referral = source.referral
    WHEN NOT MATCHED THEN
        INSERT (
                invoiceid, itemid, category, price, quantity, orderdate, destinationstate, shippingtype, referral
            )
            VALUES (
                       source.invoiceid, source.itemid,
                       source.category, source.price,
                       source.quantity, source.orderdate,
                       source.destinationstate, source.shippingtype,
                       source.referral
                   );
