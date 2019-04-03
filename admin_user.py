from jsonschema import validate, ValidationError
import json

import access_control


class AdminControl():
    def __init__(self, conn):
        with open('schema/permission_schema.json') as json_file:  
            self.schema = json.load(json_file)
        self.conn = conn

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
        acl.operator_filters = set(input["operators"]) 
        acl.munipality_filters = set(input["municipalities"])
        
        cur = self.conn.cursor()
        acl.update(cur)
        self.conn.commit()
        return acl


       
