import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5433,
    dbname='meridian',
    user='meridian_user',
    password='meridian_pass'
)
cur = conn.cursor()

with open('C:/Projects/meridian/database/schema.sql', 'r') as f:
    cur.execute(f.read())

conn.commit()
print('Schema created successfully!')

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
tables = cur.fetchall()
for t in tables:
    print(' -', t[0])

conn.close()