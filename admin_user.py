from jsonschema import validate, ValidationError
import json
import string
import random
import requests
import os

import access_control


class AdminControl():
    def __init__(self, conn):
        with open('schema/permission_schema.json') as json_file:  
            self.schema = json.load(json_file)
        self.conn = conn
        self.access_c = access_control.AccessControl(self.conn)

    def validate(self, input):
        try:
            validate(instance=input, schema=self.schema)
        except ValidationError as e:
            return e.message

    def update(self, input):
        acl = access_control.ACL(
            input["username"],
            input["filter_municipality"],
            input["filter_operator"],
            input["is_admin"]
        )
        print(input)
        print(input["municipalities"])
        acl.operator_filters = set(input["operators"]) 
        acl.municipality_filters = set(input["municipalities"])
        
        cur = self.conn.cursor()
        acl.update(cur)
        self.conn.commit()
        return acl

    def random_string_generator(self, size=10, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def create_user(self, input):
        headers = {
            'Authorization': os.getenv("FUSIONAUTH_APIKEY"),
            'Content-Type': 'application/json'
        }

        create_user_data = {}
        create_user_data["user"] = {}
        username = input["email"].lower()
        create_user_data["user"]["username"] = username
        create_user_data["user"]["email"] = username
        create_user_data["user"]["password"] = self.random_string_generator(12)

        r = requests.post("https://auth.deelfietsdashboard.nl/api/user", headers=headers, data=json.dumps(create_user_data))
        if r.status_code != 200:
            return None, ("Something went wrong during creating user %s [%s]" % (r.content, r.status_code))

        response_user = r.json()
        assign_application = {}
        assign_application["registration"] = {}
        assign_application["registration"]["applicationId"] = os.getenv("APP_ID")
        assign_application["registration"]["roles"] = [input["user_type"]]

        r = requests.post("https://auth.deelfietsdashboard.nl/api/user/registration/" + response_user["user"]["id"], headers=headers, data=json.dumps(assign_application))
        if r.status_code != 200:
            return None, ("Something went wrong during assigning role to user %s [%s]" % (r.content, r.status_code))
        response = self.create_response_user(response_user, create_user_data["user"]["password"], r.json())

        self.create_init_acl(input)

        return response, None

    def create_init_acl(self, input):
        acl = access_control.ACL(
            input["email"].lower(),
            input["user_type"] == "municipality",
            input["user_type"] == "operator",
            input["user_type"] == "administer"
        )
        cur = self.conn.cursor()
        acl.update(cur)
        self.conn.commit()


    def create_response_user(self, response_user, password, response_registration):
        res = {}
        res["username"] = response_user["user"]["username"]
        res["password"] = password
        res["roles"] = response_registration["registration"]["roles"]
        return res

    def list_users(self):
        users = self.access_c.list_acl()
        return users

    def delete_user(self, email):
        res = self.access_c.delete_user_acl(email)
        if res:
            return res

        headers = {
            'Authorization': os.getenv("FUSIONAUTH_APIKEY")
        }
        url = "https://auth.deelfietsdashboard.nl/api/user?email=%s" % email
        print(url)
        r = requests.get(url, headers=headers)
        print(r.status_code)
        print(r.json())
        if r.status_code != 200:
            return "Something went wrong, user possibly doesn't exists."
        

        user_id = r.json()["user"]["id"]
        url = "https://auth.deelfietsdashboard.nl/api/user/%s?hardDelete=true" % user_id
        r = requests.delete(url, headers=headers)
        if r.status_code != 200:
            return "Something went wrong with deleting user."
        
