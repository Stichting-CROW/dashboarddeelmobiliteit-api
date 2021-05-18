import psycopg2

def log_request(conn, username, raw_api_call, d_filter):
    cur = conn.cursor()
    stmt = """
        INSERT INTO 
        audit_log (username, timestamp_accessed, raw_api_call, filter_active) 
        VALUES (%s, NOW(), %s, %s)
        """
    cur.execute(stmt, (username, raw_api_call, d_filter.to_json()))
    conn.commit()
    