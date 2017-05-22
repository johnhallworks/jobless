import json

import requests


def http_request(**kwargs):
    try:
        print("HEREERE")
        res = requests.get(**kwargs)
        res_json = {"status_code": res.status_code,
                    "body": res.text}
        if 200 <= res_json["status_code"] <= 300:
            return True, json.dumps(res_json)
        else:
            return False, json.dumps(res_json)
    except Exception as ex:
        print("EXCEPTION: " + str(ex))
        return False, None


command_registry = {
    "http_request": http_request
}
