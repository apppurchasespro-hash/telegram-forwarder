"""
Rotate the Tencent Cloud API key used by this repo's helpers.

Approach: create a sub-user scoped to Lighthouse-only, mint an access
key for that sub-user, write it to secrets/tencent-creds.json
(replacing the current root key), and smoke-test it. The OLD root
key must then be disabled in the Tencent Cloud console — that step
isn't automatable (CAM API can manage sub-user keys but not root
keys, by design).

The new SecretKey is written straight to the creds file; it is NEVER
printed to stdout, so it can't end up in a chat transcript.
"""
import json
import sys
import time
from pathlib import Path

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.cam.v20190116 import cam_client, models as cam_models
from tencentcloud.lighthouse.v20200324 import lighthouse_client, models as lh_models


REPO = Path(__file__).resolve().parents[1]
CREDS_PATH = REPO / "secrets" / "tencent-creds.json"
SUBUSER_NAME = "tg-forwarder-deploy"
SUBUSER_REMARK = "Lighthouse-scoped key used by scripts/tencent_*.py — auto-created"
POLICY_NAMES = ["QcloudLighthouseFullAccess"]
TEST_REGION = "ap-tokyo"
TEST_INSTANCE = "lhins-b2qrwedp"


def cam(cred, action: str):
    prof = ClientProfile(httpProfile=HttpProfile(endpoint="cam.tencentcloudapi.com"))
    return cam_client.CamClient(cred, "", prof)


def get_or_create_subuser(cred) -> int:
    client = cam(cred, "")
    # Look first — ListUsers is cheap
    rq = cam_models.ListUsersRequest()
    rq.from_json_string("{}")
    data = json.loads(client.ListUsers(rq).to_json_string())
    for u in data.get("Data", []) or []:
        if u.get("Name") == SUBUSER_NAME:
            print(f"sub-user already exists: {SUBUSER_NAME} (uin={u['Uin']})")
            return u["Uin"]
    # Create
    rq = cam_models.AddUserRequest()
    rq.from_json_string(json.dumps({
        "Name": SUBUSER_NAME,
        "Remark": SUBUSER_REMARK,
        "ConsoleLogin": 0,             # API access only
        "UseApi": 1,
        "Password": "",
    }))
    resp = json.loads(client.AddUser(rq).to_json_string())
    uin = resp["Uin"]
    print(f"created sub-user: {SUBUSER_NAME} (uin={uin})")
    return uin


def lookup_policy_ids(cred, names: list[str]) -> dict[str, int]:
    client = cam(cred, "")
    found: dict[str, int] = {}
    page = 1
    while names and len(found) < len(names):
        rq = cam_models.ListPoliciesRequest()
        rq.from_json_string(json.dumps({"Rp": 200, "Page": page, "Scope": "QCS"}))
        data = json.loads(client.ListPolicies(rq).to_json_string())
        items = data.get("List", []) or []
        for p in items:
            if p.get("PolicyName") in names and p["PolicyName"] not in found:
                found[p["PolicyName"]] = p["PolicyId"]
        if len(items) < 200:
            break
        page += 1
    return found


def attach_policies(cred, uin: int, policy_ids: list[int]):
    client = cam(cred, "")
    for pid in policy_ids:
        rq = cam_models.AttachUserPolicyRequest()
        rq.from_json_string(json.dumps({"PolicyId": pid, "AttachUin": uin}))
        try:
            client.AttachUserPolicy(rq)
            print(f"attached policy {pid} to uin {uin}")
        except Exception as e:
            # 'PolicyId already attached' is fine
            if "already" in repr(e).lower():
                print(f"policy {pid} already attached")
            else:
                raise


def mint_access_key(cred, uin: int) -> tuple[str, str]:
    client = cam(cred, "")
    rq = cam_models.CreateAccessKeyRequest()
    rq.from_json_string(json.dumps({"TargetUin": uin}))
    data = json.loads(client.CreateAccessKey(rq).to_json_string())
    ak = data["AccessKey"]
    return ak["AccessKeyId"], ak["SecretAccessKey"]


def write_creds_file(new_id: str, new_key: str, old_id: str):
    payload = {
        "SecretId": new_id,
        "SecretKey": new_key,
        "note": f"Rotated {time.strftime('%Y-%m-%d %H:%M:%S')}. Sub-user '{SUBUSER_NAME}' (Lighthouse scope). Old key {old_id[:10]}... must be disabled manually in the Tencent Cloud console.",
    }
    backup = CREDS_PATH.with_suffix(".json.bak")
    if CREDS_PATH.exists():
        backup.write_text(CREDS_PATH.read_text())
    CREDS_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote new creds to {CREDS_PATH}")
    if backup.exists():
        print(f"backed up old creds to {backup} (delete after confirming new key works)")


def smoke_test(new_id: str, new_key: str) -> bool:
    cred = credential.Credential(new_id, new_key)
    prof = ClientProfile(httpProfile=HttpProfile(endpoint="lighthouse.tencentcloudapi.com"))
    client = lighthouse_client.LighthouseClient(cred, TEST_REGION, prof)
    try:
        rq = lh_models.DescribeFirewallRulesRequest()
        rq.from_json_string(json.dumps({"InstanceId": TEST_INSTANCE}))
        data = json.loads(client.DescribeFirewallRules(rq).to_json_string())
        n = data.get("TotalCount", 0)
        print(f"smoke test OK: new key can DescribeFirewallRules on {TEST_INSTANCE} ({n} rules)")
        return True
    except Exception as e:
        print(f"smoke test FAILED: {repr(e)[:300]}")
        return False


def main():
    if not CREDS_PATH.exists():
        sys.exit(f"creds file missing: {CREDS_PATH}")
    old = json.loads(CREDS_PATH.read_text())
    cred = credential.Credential(old["SecretId"], old["SecretKey"])
    old_id = old["SecretId"]

    print(f"using current key: {old_id[:10]}... to provision the rotation")
    uin = get_or_create_subuser(cred)

    policies = lookup_policy_ids(cred, POLICY_NAMES)
    missing = [n for n in POLICY_NAMES if n not in policies]
    if missing:
        sys.exit(f"could not find policies: {missing}")
    print(f"resolved policy ids: {policies}")
    attach_policies(cred, uin, list(policies.values()))

    new_id, new_key = mint_access_key(cred, uin)
    print(f"new SecretId: {new_id}")
    # SecretKey deliberately NOT printed

    write_creds_file(new_id, new_key, old_id)
    if not smoke_test(new_id, new_key):
        sys.exit("ABORTED: new key failed smoke test. Old key still in backup file; investigate.")

    print()
    print("=" * 60)
    print("ROTATION DONE — but the OLD root key is still valid.")
    print(f"Old SecretId: {old_id}")
    print("Disable it now in the Tencent Cloud console:")
    print("  https://console.cloud.tencent.com/cam/capi")
    print("  -> find the row starting with the SecretId above")
    print("  -> click 'Disable', then later 'Delete'")
    print("=" * 60)


if __name__ == "__main__":
    main()
