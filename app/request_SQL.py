GET_MAX_DZ = """
    SELECT departure_zid, ts
    FROM (SELECT departure_zid, ts, max(summ)
        FROM (SELECT  departure_zid, ts, sum(customers_cnt) + sum(customers_cnt_metro) as summ
            FROM matrix 
            WHERE (STRFTIME('%w', ts) in ('1', '2', '3', '4', '5'))
                AND (STRFTIME('%H:%M', ts) BETWEEN '00:00' AND '12:00')
            GROUP BY departure_zid, ts)
        GROUP BY departure_zid
        UNION ALL
        SELECT departure_zid, ts, max(summ)
        FROM (SELECT  departure_zid, ts, sum(customers_cnt) + sum(customers_cnt_metro) as summ
            FROM matrix
            WHERE (STRFTIME('%w', ts) in ('1', '2', '3', '4', '5'))
                AND (STRFTIME('%H:%M', ts) BETWEEN '12:01' AND '23:59')
            GROUP BY departure_zid, ts)
        GROUP BY departure_zid)
    ORDER BY departure_zid;
    """

GET_MAX_CITY = """
    SELECT ts
    FROM(SELECT ts, max(summ)
    FROM (SELECT ts, sum(customers_cnt) + sum(matrix.customers_cnt_metro) as summ
        FROM matrix
        WHERE (STRFTIME('%w', ts) in ('1', '2', '3', '4', '5'))
            AND (STRFTIME('%H:%M', ts) BETWEEN '00:00' AND '12:00')
        group by ts)
    UNION ALL
    SELECT ts, max(summ)
    FROM (SELECT ts, sum(customers_cnt) + sum(matrix.customers_cnt_metro) as summ
    FROM matrix
    WHERE (STRFTIME('%w', ts) in ('1', '2', '3', '4', '5'))
        AND (STRFTIME('%H:%M', ts) BETWEEN '00:00' AND '12:00')
    group by ts));
"""