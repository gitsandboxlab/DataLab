import pyodbc
import pandas as pd
import re
import operator

OPS = {'>=': operator.ge, '<=': operator.le, '!=': operator.ne,
       '=': operator.eq, '>': operator.gt, '<': operator.lt}

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

results = []

for idx, row in df.iterrows():
    cursor.execute(row['TargetQuery'])
    actual_count = cursor.fetchone()[0]

    op_str, num = re.match(r'(>=|<=|!=|=|>|<)\s*(-?\d+)', row['PassCondition'].strip()).groups()
    passed = OPS[op_str](actual_count, int(num))

    results.append({**row.to_dict(), 'ActualCount': actual_count, 'Passed': passed})

results_df = pd.DataFrame(results)
