import jwt

class AccessControl():
    def __init__(self, conn):
        self.conn = conn
    
    def retrieve_acl_user(self, request):
        if not request.headers.get('Authorization'):
            return None
        encoded_token = request.headers.get('Authorization')
        encoded_token = encoded_token.split(" ")[1]
        # Verification is performed by kong (reverse proxy), 
        # therefore token is not verified for a second time so that the secret is only stored there.
        result = jwt.decode(encoded_token, verify=False)

        # Get ACL and return result
        return self.query_acl(result["email"])

    def query_acl(self, email):
        stmt = """
        SELECT username, filter_municipality, filter_operator, is_admin
        FROM acl
        WHERE username=%s;
        """
        cur = self.conn.cursor()
        cur.execute(stmt, (email,))
        if cur.rowcount < 1:
            return None
        
        user = cur.fetchone()
        acl_user = ACL(user[0], user[1], user[2], user[3])
        acl_user.retrieve_municipalities(cur)
        acl_user.retrieve_operators(cur)
        return acl_user

    def list_acl(self):
        stmt = """
        SELECT username, filter_municipality, filter_operator, is_admin
        FROM acl;
        """
        cur = self.conn.cursor()
        cur.execute(stmt)

        users = []
        for user in cur.fetchall():
            users.append(ACL(user[0], user[1], user[2], user[3]))
        return users

    def delete_user_acl(self, email):
        cur = self.conn.cursor()
        user_acl = self.query_acl(email)
        if not user_acl:
            return "User doesn't exists in ACL."
        user_acl.operator_filters = set()
        user_acl.update_operator(cur)
        user_acl.municipality_filters = set()
        user_acl.update_municipality(cur)
        user_acl.delete(cur)
        self.conn.commit()

class ACL():
    def __init__(self, username, has_municipality_filter_enabled, 
            has_operator_filter_enabled, is_administrator):
        self.username = username
        self.has_municipality_filter_enabled = has_municipality_filter_enabled
        self.has_operator_filter_enabled = has_operator_filter_enabled
        self.is_administrator = is_administrator
        self.operator_filters = set()
        self.municipality_filters = set()
        self.hr_municipality_filters = []
        self.zone_filters = set()

    def check_municipality_code(self, municipality_code):
        if not self.has_municipality_filter():
            return True, None

        if municipality_code in self.municipality_filters:
            return True, None

        return False, "User is not allowed to access this municipality."

    # This function returns true when user has right to access.
    def check_municipalities(self, d_filter):
        if not self.has_municipality_filter():
            return True, None

        if not d_filter.has_zone_filter():
            return False, "No zone filter was specified, but user doesn't have access to all zones."

        for zone_id in d_filter.get_zones():
            if zone_id not in self.zone_filters:
                return False, "User is not allowed to access data within zone %s" % zone_id

        return True, None

    # This function returns true when user has right to access.
    def check_operators(self, d_filter):
        if not self.has_operator_filter():
            return True, None

        if not d_filter.has_operator_filter():
            return False, "No operator filter was specified, but user doesn't have access to all operators."
        
        for operator in d_filter.get_operators():
            if operator not in self.operator_filters:
                return False, "User is not allowed to access operator %s" % operator

        return True, None

    def is_authorized(self, d_filter):
        is_authorized, error = self.check_municipalities(d_filter)
        if not is_authorized:
            return False, error
        
        is_authorized, error = self.check_operators(d_filter)
        if not is_authorized:
            return False, error

        return True, None

    def has_operator_filter(self):
        return self.has_operator_filter_enabled

    def has_municipality_filter(self):
        return self.has_municipality_filter_enabled

    def is_admin(self):
        return self.is_administrator

    # Retrieve the municipalities to filter on.
    def retrieve_municipalities(self, cur):
        if not self.has_municipality_filter():
            return
        stmt = """SELECT acl_municipalities.municipality, name
            FROM acl_municipalities
            LEFT JOIN zones
            ON acl_municipalities.municipality = zones.municipality
            WHERE username = %s and zones.zone_type = 'municipality'"""
        cur.execute(stmt, (self.username,))
        results = cur.fetchall()
        for item in results:
            self.municipality_filters.add(item[0])
            self.hr_municipality_filters.append({"gm_code": item[0], "name": item[1]})
        self.retrieve_zones(cur)

    def retrieve_zones(self, cur):
        if len(self.municipality_filters) == 0:
            return
        stmt = """SELECT zone_id
            FROM zones
            where municipality in %s"""
        cur.execute(stmt, (tuple(self.municipality_filters),))
        for item in cur.fetchall():
            self.zone_filters.add(str(item[0]))
            
    # Retrieve the operators to filter on.
    def retrieve_operators(self, cur):
        if not self.has_operator_filter():
            return
        stmt = """SELECT operator
            FROM acl_operator
            WHERE username = %s"""
        cur.execute(stmt, (self.username,))
        results = cur.fetchall()
        for item in results:
            self.operator_filters.add(item[0])

    def update(self, cur):
        stmt = """
            INSERT INTO acl (username, filter_municipality, 
                filter_operator, is_admin)
            VALUES
            (%s, %s, %s, %s) 
            ON CONFLICT (username) 
            DO
            UPDATE
            SET username = EXCLUDED.username,
            filter_municipality = EXCLUDED.filter_municipality,
            filter_operator = EXCLUDED.filter_operator,
            is_admin = EXCLUDED.is_admin
            """
        cur.execute(stmt, (self.username, self.has_municipality_filter(),
            self.has_operator_filter(), self.is_admin()))
        self.update_municipality(cur)
        self.update_operator(cur)


    def update_municipality(self, cur):
        stmt = """DELETE FROM acl_municipalities 
            WHERE username = %s"""
        cur.execute(stmt, (self.username,))
        
        stmt2 = """INSERT INTO acl_municipalities
            (username, municipality)
            VALUES (%s, %s)"""

        for municipality in self.municipality_filters:
            cur.execute(stmt2, (self.username, municipality))

    def update_operator(self, cur):
        stmt = """DELETE FROM acl_operator
            WHERE username = %s"""
        cur.execute(stmt, (self.username,))

        stmt2 = """INSERT INTO acl_operator
            (username, operator)
            VALUES (%s, %s)"""

        for operator in self.operator_filters:
            cur.execute(stmt2, (self.username, operator))

    def delete(self, cur):
        stmt = """DELETE FROM acl
            WHERE username = %s"""
        cur.execute(stmt, (self.username,))

    def serialize(self):
        data = {}
        data["username"] = self.username
        data["is_admin"] = self.is_admin()
        data["filter_municipality"] = self.has_municipality_filter_enabled
        data["filter_operator"] = self.has_operator_filter_enabled
        data["municipalities"] = self.municipality_filters
        data["operators"] = self.operator_filters
        return data 

    def human_readable_serialize(self):
        data = self.serialize()
        
        municipalities = []
        if self.has_municipality_filter():
            municipalities = self.hr_municipality_filters
        else: 
            municipalities = self.default_municipalities()
        data["municipalities"] = municipalities

        operators = []
        if self.has_operator_filter():
            for operator in self.operator_filters:
                operators.append({"system_id": operator, "name": operator.capitalize()})
        else:
            operators = self.default_operators()
        data["operators"] = operators
        return data

    # Temporary static list of municipalities, that should be shown when filtering on municipalities is not enforced.
    def default_municipalities(self):
        data = []
        data.append({"gm_code": "GM0362", "name": "Amstelveen"})
        data.append({"gm_code": "GM0394", "name": "Haarlemermeer"})
        data.append({"gm_code": "GM0599", "name": "Rotterdam"})
        data.append({"gm_code": "GM0518", "name": "Den Haag"})
        data.append({"gm_code": "GM0344", "name": "Utrecht"})
        data.append({"gm_code": "GM0479", "name": "Zaanstad"})
        data.append({"gm_code": "GM0363", "name": "Amsterdam"})
        data.append({"gm_code": "GM0503", "name": "Delft"})
        data.append({"gm_code": "GM0289", "name": "Wageningen"})
        data.append({"gm_code": "GM0228", "name": "Ede"})
        data.append({"gm_code": "GM0794", "name": "Helmond"})
        data.append({"gm_code": "GM0772", "name": "Eindhoven"})
        data.append({"gm_code": "GM0193", "name": "Zwolle"})
        data.append({"gm_code": "GM0606", "name": "Schiedam"})
        data.append({"gm_code": "GM0758", "name": "Breda"})
        return data

    # Temporary static list of operators, should be shown when filtering on operator is not enforced.
    def default_operators(self):
        operators = []
        operators.append({"system_id": "cykl", "name": "Cykl"})
        operators.append({"system_id": "flickbike", "name": "Flickbike"})
        operators.append({"system_id": "donkey", "name": "Donkey Republic"})
        operators.append({"system_id": "mobike", "name": "Mobike"})
        operators.append({"system_id": "htm", "name": "HTM"})
        operators.append({"system_id": "jump", "name": "JUMP (Uber)"})
        operators.append({"system_id": "gosharing", "name": "GO Sharing"})
        operators.append({"system_id": "check", "name": "CHECK"})
        operators.append({"system_id": "felyx", "name": "Felyx"})
        operators.append({"system_id": "deelfietsnederland", "name": "Deelfiets Nederland"})
        return operators


        
