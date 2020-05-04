import psycopg2


class DBMSFinalProject:
    def __init__(self, connection_string):
        self.conn = psycopg2.connect(connection_string)

    def precipitation_vs_num_incidents(self):
        query = """
        SELECT t1.zipcode as zip, incids, SUM(precip) as precip
        FROM
            (SELECT zip5 as zipcode, COUNT(*) as incids
            FROM incident_address
            GROUP BY zipcode) as t1
        JOIN
            closest_station
            ON t1.zipcode=closest_station.zipcode
        JOIN
            (SELECT stnId, SUM(value) as precip
            FROM measurement
            GROUP BY stnId) as t2
            ON t2.stnId=closest_station.stnId
        GROUP BY t2.stnId;
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
            return res

    def property_damage_vs_precipitation(self):
        query = """
        SELECT prop_loss, precip
        FROM
            (SELECT prop_loss, ZIP5 as zipcode, incident_address.date as date
            FROM inicident
            JOIN basic_incident
                ON incident_address.inc_no=basic_incident.inc_no
            ) as t1
        JOIN
            closest_station
            ON t1.zipcode=closest_station.zipcode
        JOIN
            (SELECT stnId, value as precip
            FROM measurement
            GROUP BY stnId) as t2
            ON
                t2.stnId = closest_station.stnId
                AND
                DATE_PART('day', t2.date - t1.date) * 24 + DATE_PART('hour', t2.date - t1.date)
                =
                (SELECT MIN(DATE_PART('day', m.date - i.date) * 24 + DATE_PART('hour', m.date - i.date))
                FROM measurement as m
                JOIN closest_station as c
                    ON m.stnId=c.stnId
                JOIN incident_address as i
                    ON i.zipcode=c.zipcode)
        ORDER BY prop_loss DESC;
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
            return res

    def precipitation_by_city(self, city, state):
        query = """
        SELECT SUM(m.value) as precip
        FROM measurement as m
        JOIN closest_station as c
            ON m.stnId=c.stnId
        JOIN incident_address as i
            ON i.ZIP5=c.zipcode
        WHERE i.city ILIKE %s AND i.state ILIKE %s AND m.value > 0;
        """

        with self.conn.cursor() as cur:
            cur.execute(query, (city, state))
            res = cur.fetchone()
            return res

    def total_precipitation_by_state(self):
        query = """
        SELECT state, SUM(value) as precip
        FROM incident_address
        JOIN closest_station
            ON incident_address.ZIP5=closest_station.zipcode
        JOIN measurement
            ON incident_address.stnId = closest_station.stnId
        GROUP BY state
        ORDER BY state;
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
            return res

    def total_incidents_by_city(self, city, state):
        query = """
        SELECT COUNT(*)
        FROM incident_address
        WHERE city ILIKE %s AND state ILIKE %s;
        """

        with self.conn.cursor() as cur:
            cur.execute(query, (city, state))
            res = cur.fetchall()
            return res
