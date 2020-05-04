

def precipitation_vs_incidents():
    return \
    """SELECT t1.zipcode as zip, incids, SUM(precip) as precip
        FROM
            (SELECT zipcode, COUNT(*) as incids
            FROM incidents
            GROUP BY zipcode) as t1
        JOIN
            ClosestStation
            ON t1.zipcode=ClosestStation.zipcode
        JOIN
            (SELECT stnId, SUM(value) as precip
            FROM measurement
            GROUP BY stnId) as t2
            ON t2.stnId=ClosestStation.stnId
        GROUP BY t2.stnId;
        """


def precipitation_by_city():
    return \
    """SELECT SUM(m.value) as precip
        FROM measurement as m
        JOIN ClosestStation as c
            ON m.stnId=c.stnId
        JOIN incident as i
            ON i.zipcode=c.zipcode
        WHERE i.city='%s';
    """


def precipitation_vs_property_damage():
    return \
    """SELECT prop_loss, precip
        FROM
            (SELECT prop_loss, zipcode, incident.date as date
            FROM inicident
            JOIN basicincident
                ON incident.inc_no=basicincident.inc_no
            ) as t1
        JOIN
            ClosestStation
            ON t1.zipcode=ClosestStation.zipcode
        JOIN
            (SELECT stnId, value as precip
            FROM measurement
            GROUP BY stnId) as t2
            ON
                t2.stnId = ClosestStation.stnId
                AND
                DATE_PART('day', t2.date - t1.date) * 24 + DATE_PART('hour', t2.date - t1.date)
                =
                (SELECT MIN(DATE_PART('day', m.date - i.date) * 24 + DATE_PART('hour', m.date - i.date))
                FROM measurement as m
                JOIN ClosestStation as c
                    ON m.stnId=c.stnId
                JOIN incident as i
                    ON i.zipcode=c.zipcode)
        ORDER BY prop_loss DESC;
        """

def precipitation_by_state():
    return \
    """SELECT state, SUM(value) as precip
        FROM incident
        JOIN ClosestStation
            ON t1.zipcode=ClosestStation.zipcode
        JOIN measurement
            ON t2.stnId = ClosestStation.stnId
        GROUP BY state
        ORDER BY state;
    """


def incidents_by_city():
    return \
    """SELECT COUNT(*)
        FROM incident
        WHERE city='%s';
        """