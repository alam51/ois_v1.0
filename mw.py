import datetime
from utils import CONNECTOR
import mysql.connector
import pandas as pd

t1 = datetime.datetime.now()

from_datetime_str = '2022-3-1'
to_datetime_str = '2022-3-31 23:00'

transformer_33kv_max_query_str = f"""
SELECT s.id AS ss_id, MW.date_time, max(abs(MW.value)) as MW
-- tt.voltage_level, se.is_transformer_low
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN transformer AS t ON se.transformer_id = t.id
JOIN transformer_type AS tt ON tt.id = t.type_id
JOIN substation AS s ON se.substation_id = s.id
WHERE se.is_transformer_low = 1
AND (tt.id = 1 OR tt.id = 8)
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
GROUP BY s.id
"""

generation_33kv_max_str = f"""
SELECT s.id AS ss_id, MW.date_time, max(abs(MW.value)) AS MW
-- f.is_generation, se.is_feeder
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN feeder AS f ON se.feeder_id = f.id
JOIN substation AS s ON se.substation_id = s.id
-- JOIN zone AS z ON z.id = s.zone
-- JOIN gmd ON gmd.id = s.gmd
WHERE f.is_generation = 1
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
GROUP BY s.id
"""

ss_query_str = """
SELECT s.id AS ss_id, s.name AS ss, zone.name AS zone, gmd.name AS gmd
FROM
substation AS s
JOIN zone ON s.zone = zone.id
JOIN gmd ON s.gmd = gmd.id
"""

transformer_33kv_max_df = pd.read_sql_query(transformer_33kv_max_query_str, CONNECTOR, index_col='ss_id')
generation_33kv_max_df = pd.read_sql_query(generation_33kv_max_str, CONNECTOR, index_col='ss_id')
ss_df1 = pd.read_sql_query(ss_query_str, CONNECTOR, index_col='ss_id')


def is_33_present(_str):
    return '/33' in _str


ss_df = ss_df1[ss_df1.ss.map(is_33_present)]

for i in generation_33kv_max_df.index:
    transformer_33kv_max_df.loc[i, 'MW'] += generation_33kv_max_df.loc[i, 'MW']

ss_max_mw_df = ss_df.join(transformer_33kv_max_df).sort_values(by=['zone', 'gmd'], ignore_index=True)
ss_max_mw_df.to_excel('max_mw.xlsx')
print(f'time elapsed = {datetime.datetime.now() - t1}')
a = 5
