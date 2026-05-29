"""Identify whose Tencent Cloud account owns the API key in .tencent-creds.local.json."""
import json
from pathlib import Path

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile

CREDS_PATH = Path(__file__).resolve().parents[1] / "secrets" / "tencent-creds.json"
c = json.loads(CREDS_PATH.read_text())
cred = credential.Credential(c["SecretId"], c["SecretKey"])

try:
    from tencentcloud.cam.v20190116 import cam_client, models as cam_models

    prof = ClientProfile(httpProfile=HttpProfile(endpoint="cam.tencentcloudapi.com"))
    client = cam_client.CamClient(cred, "", prof)
    req = cam_models.GetUserAppIdRequest()
    req.from_json_string("{}")
    print("GetUserAppId:", client.GetUserAppId(req).to_json_string())
except Exception as e:
    print("GetUserAppId err:", repr(e)[:300])

try:
    from tencentcloud.cam.v20190116 import cam_client, models as cam_models

    prof = ClientProfile(httpProfile=HttpProfile(endpoint="cam.tencentcloudapi.com"))
    client = cam_client.CamClient(cred, "", prof)
    req = cam_models.ListUsersRequest()
    req.from_json_string("{}")
    print("ListUsers:", client.ListUsers(req).to_json_string()[:500])
except Exception as e:
    print("ListUsers err:", repr(e)[:300])
