import json

import requests


def http_request(**kwargs):
    try:
        res = requests.request(**kwargs)
        res_json = {"status_code": res.status_code,
                    "body": res.text}
        if 200 <= res_json["status_code"] <= 300:
            return True, json.dumps(res_json)
        else:
            return False, json.dumps(res_json)
    except Exception as ex:
        print("EXCEPTION: " + str(ex))
        res_json = {"status_code": None,
                    "body": str(ex)}
        return False, json.dumps(res_json)


command_registry = {
    "http_request": http_request
}
