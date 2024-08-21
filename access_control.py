import jwt

class AccessControl():
    def retrieve_acl_user(self, request, conn):
        user_id = None
        consumer_username = request.headers.get("X-Consumer-Username")
        if request.headers.get('Authorization'):
            user_id = self.get_user_id_jwt(request.headers.get('Authorization'))
        elif consumer_username and consumer_username != "anonymous":
            user_id = consumer_username
        if not user_id:
            return None

        # Get ACL and return result
        return self.query_acl(conn, user_id)
    
    def get_user_id_jwt(self, encoded_token):
        encoded_token = encoded_token.split(" ")[1]
        # Verification is performed by kong (reverse proxy), 
        # therefore token is not verified for a second time so that the secret is only stored there.
        result = jwt.decode(encoded_token, verify=False)
        return result["email"]

    def query_acl(self, conn, email):
        stmt = """
        SELECT user_id, organisation_id, type_of_organisation, privileges                                                    
        FROM user_account 
        JOIN organisation USING (organisation_id) 
        WHERE user_id = %s;
        """
        cur = conn.cursor()
        cur.execute(stmt, (email,))
        print(email)
        if cur.rowcount < 1:
            return None
        
        user = cur.fetchone()
        print(user)
        acl_user = ACL(user[0], user[1], user[2], user[3])
        acl_user.retrieve_municipalities(cur)
        acl_user.retrieve_operators(cur)
        return acl_user


class ACL():
    def __init__(self, username, organisation_id, organisation_type, privileges):
        self.username = username
        self.organisation_id = organisation_id
        self.organisation_type = organisation_type
        self.privileges = privileges
        self.operator_filters = set()
        self.municipality_filters = set()
        self.hr_municipality_filters = []
        self.zone_filters = set()
        self.default_acl = DefaultACL()

    def check_municipality_code(self, municipality_code):
        if len(self.municipality_filters) == 0:
            return True, None

        if municipality_code in self.municipality_filters:
            return True, None

        return False, "User is not allowed to access this municipality."

    # This function returns true when user has right to access.
    def check_municipalities(self, d_filter):
        if len(self.municipality_filters) == 0:
            return True, None

        if not d_filter.has_zone_filter():
            return False, "No zone filter was specified, but user doesn't have access to all zones."

        for zone_id in d_filter.get_zones():
            if zone_id not in self.zone_filters:
                return False, "User is not allowed to access data within zone %s" % zone_id

        return True, None

    # This function returns true when user has right to access.
    def check_operators(self, d_filter):
        if len(self.operator_filters) == 0:
            return True, None

        if not d_filter.has_operator_filter():
            return False, "No operator filter was specified, but user doesn't have access to all operators."
        
        for operator in d_filter.get_operators():
            if operator not in self.operator_filters:
                return False, "User is not allowed to access operator %s" % operator

        return True, None

    def is_authorized(self, d_filter):
        if self.organisation_type != "ADMIN" and len(self.operator_filters) == 0 and len(self.municipality_filters) == 0:
            return False, "There should be at least a filter on municipalities or operators."

        is_authorized, error = self.check_municipalities(d_filter)
        if not is_authorized:
            return False, error
        
        is_authorized, error = self.check_operators(d_filter)
        if not is_authorized:
            return False, error

        return True, None
    
    def is_authorized_for_raw_data(self):
        return self.organisation_type == "ADMIN" or "DOWNLOAD_RAW_DATA" in self.privileges 

    # Retrieve the municipalities to filter on.
    def retrieve_municipalities(self, cur):
        stmt = """
        SELECT municipality, name
        FROM
            (SELECT UNNEST(data_owner_of_municipalities) as municipalities
            FROM organisation
            WHERE organisation_id IN(
                SELECT DISTINCT(owner_organisation_id) 
                FROM view_data_access 
                WHERE granted_organisation_id = %(organisation_id)s
                OR granted_user = %(user_id)s
            )
            OR organisation_id = %(organisation_id)s) as q1
        LEFT JOIN zones
        ON q1.municipalities = zones.municipality
        WHERE zones.zone_type = 'municipality';"""
        cur.execute(stmt, {
            "user_id": self.username, 
            "organisation_id": self.organisation_id
        })
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
        stmt = """SELECT UNNEST(data_owner_of_operators) as operators
            FROM organisation
            WHERE organisation_id IN(
                SELECT DISTINCT(owner_organisation_id) 
                FROM view_data_access 
                WHERE granted_organisation_id = %(organisation_id)s
                OR granted_user = %(user_id)s
            )
            OR organisation_id = %(organisation_id)s;"""
        cur.execute(stmt, {
            "user_id": self.username, 
            "organisation_id": self.organisation_id
        })
        results = cur.fetchall()
        for item in results:
            self.operator_filters.add(item[0])

    def serialize(self):
        data = {}
        data["username"] = self.username
        data["organisation_type"] = self.organisation_type
        data["privileges"] = self.privileges
        data["is_admin"] = self.organisation_type == "ADMIN"
        data["filter_municipality"] = False # self.has_municipality_filter_enabled
        data["filter_operator"] = False # self.has_operator_filter_enabled
        data["is_contact_person_municipality"] = "ORGANISATION_ADMIN" in self.privileges
        data["municipalities"] = self.municipality_filters
        data["operators"] = self.operator_filters
        return data 

    def human_readable_serialize(self, cur):
        data = self.serialize()
        
        municipalities = []
        if len(self.municipality_filters) > 0 and len(self.operator_filters) == 0:
            municipalities = self.hr_municipality_filters
        else: 
            municipalities = self.default_acl.default_municipalities(cur)
        data["municipalities"] = municipalities

        operators = []
        if len(self.operator_filters) > 0 and len(self.municipality_filters) == 0:
            for operator in self.operator_filters:
                operators.append({"system_id": operator, "name": operator.capitalize()})
        else:
            operators = self.default_acl.default_operators()
        data["operators"] = operators
        return data
        
class DefaultACL:
    # Temporary static list of municipalities, that should be shown when filtering on municipalities is not enforced.
    def default_municipalities(self, cur):
        data = []
        stmt = """
            SELECT municipalities_with_data.name, municipality, zone_id
            FROM municipalities_with_data 
            JOIN zones
            USING(municipality)
            WHERE zone_type = 'municipality'
            ORDER BY name;
        """
        cur.execute(stmt)
        for municipality in cur.fetchall():
            data.append({"gm_code": municipality[1], "name": municipality[0], "zone_id": municipality[2]})
        return data

    # Static list of operators, should be shown when filtering on operator is not enforced.
    def default_operators(self):
        operators = []
        operators.append({"system_id": "cykl", "name": "Cykl"})
        operators.append({"system_id": "donkey", "name": "Donkey Republic"})
        operators.append({"system_id": "mobike", "name": "Mobike"})
        operators.append({"system_id": "htm", "name": "HTM"})
        operators.append({"system_id": "gosharing", "name": "GO Sharing"})
        operators.append({"system_id": "check", "name": "CHECK"})
        operators.append({"system_id": "felyx", "name": "Felyx"})
        operators.append({"system_id": "deelfietsnederland", "name": "Deelfiets Nederland"})
        operators.append({"system_id": "keobike", "name": "Keobike"})
        operators.append({"system_id": "lime", "name": "Lime"})
        operators.append({"system_id": "baqme", "name": "BAQME"})
        operators.append({"system_id": "cargoroo", "name": "Cargoroo"})
        operators.append({"system_id": "uwdeelfiets", "name": "uwdeelfiets"})
        operators.append({"system_id": "hely", "name": "Hely"})
        operators.append({"system_id": "tier", "name": "TIER"})
        operators.append({"system_id": "bolt", "name": "Bolt"})
        operators.append({"system_id": "bondi", "name": "bondi"})
        operators.append({"system_id": "dott", "name": "Dott"})
        operators.append({"system_id": "moveyou", "name": "GoAbout"})
        return operators

    def serialize(self, conn):
        data = {}
        municipalities = self.default_municipalities(conn.cursor())
        data["municipalities"] = municipalities
        operators = self.default_operators()
        data["operators"] = operators
        data["zones"] = []
        return data
