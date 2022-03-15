import hashlib

def create_sha1_hash(str): 
  str += ":dashboarddeelmobiliteit"
  # encode the string
  encoded_str = str.encode()
  # create a sha1 hash object initialized with the encoded string
  hash_obj = hashlib.sha1(encoded_str)
  # convert the hash object to a hexadecimal value
  hexa_value = hash_obj.hexdigest()
  # print
  return hexa_value

def register_active_user(conn, user):
  if not conn:
    return False

  if not user["username"]:
    return False

  role = None
  if user["is_admin"]:
    role = 'admin'
  elif user["filter_operator"]: 
    role = 'operator'
  elif user["filter_municipality"]:
    role = 'municipality'

  cur = conn.cursor()
  stmt = """
      INSERT INTO active_user_stats
      (user_hash, role, active_on)
      VALUES (%s, %s, NOW())
      ON CONFLICT ON CONSTRAINT active_user_on_date
      DO NOTHING;
  """
  try:
      cur.execute(stmt, (create_sha1_hash(user["username"]), role))
      conn.commit()
  except Exception as e:
      print(e)
      conn.rollback()
  return True
