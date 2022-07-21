CREATE OR REPLACE VIEW pbi_query AS
    SELECT t.idtrip, t.idvendor, c.date, c.day, c.day_of_week, EXTRACT(HOUR FROM t.pu_time) as hour, pu.zone as pickup_zone, pu.borough as pickup_borough,
            doz.zone as dropoff_zone, doz.borough as dropoff_borough, t.duration, t.distance, t.temperature, pt.precip_type, p.idrate_code, p.idpayment_type, p.total_amount
    FROM (
        SELECT * FROM trip
        LEFT JOIN aux_outlier
        ON idtrip = idrecord
        WHERE idrecord IS NULL
        ) as t
    LEFT JOIN calendar as c USING (iddate)
    LEFT JOIN precip_type as pt USING (idprecip_type)
    LEFT JOIN payment as p USING (idtrip)
    LEFT JOIN (
                SELECT t.idtrip, z.zone, b.borough
                FROM trip as t
                LEFT JOIN zone as z
                ON t.pu_idzone = z.idzone
                LEFT JOIN borough as b USING (idborough)
                ) as pu USING (idtrip)
    LEFT JOIN (
                SELECT t.idtrip, z.zone, b.borough
                FROM trip as t
                LEFT JOIN zone as z
                ON t.do_idzone = z.idzone
                LEFT JOIN borough as b USING (idborough)
                ) as doz USING (idtrip)
    ORDER BY t.idtrip;