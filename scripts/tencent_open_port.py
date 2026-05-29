"""
Open a TCP port on the Tencent Cloud security group attached to a CVM instance.

Usage:
  python scripts/tencent_open_port.py <instance_id> <port> [--cidr 0.0.0.0/0] [--desc "label"]

Reads creds from .tencent-creds.local.json (gitignored). Region is read from
the instance via DescribeInstances, so this works across regions.
"""
import argparse
import json
import sys
from pathlib import Path

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.cvm.v20170312 import cvm_client, models as cvm_models
from tencentcloud.vpc.v20170312 import vpc_client, models as vpc_models


CREDS_PATH = Path(__file__).resolve().parents[1] / "secrets" / "tencent-creds.json"


def load_creds():
    if not CREDS_PATH.exists():
        sys.exit(f"creds file missing: {CREDS_PATH}")
    with CREDS_PATH.open() as f:
        c = json.load(f)
    return credential.Credential(c["SecretId"], c["SecretKey"])


def find_instance(cred, region: str, instance_id: str):
    prof = ClientProfile(httpProfile=HttpProfile(endpoint="cvm.tencentcloudapi.com"))
    client = cvm_client.CvmClient(cred, region, prof)
    req = cvm_models.DescribeInstancesRequest()
    req.from_json_string(json.dumps({"InstanceIds": [instance_id]}))
    resp = client.DescribeInstances(req)
    data = json.loads(resp.to_json_string())
    if not data.get("InstanceSet"):
        return None
    return data["InstanceSet"][0]


def search_regions_for_instance(cred, instance_id: str, regions: list[str]):
    for r in regions:
        try:
            inst = find_instance(cred, r, instance_id)
            if inst:
                return r, inst
        except Exception:
            pass
    return None, None


def add_ingress_rule(cred, region: str, sg_id: str, port: int, cidr: str, desc: str):
    prof = ClientProfile(httpProfile=HttpProfile(endpoint="vpc.tencentcloudapi.com"))
    client = vpc_client.VpcClient(cred, region, prof)
    req = vpc_models.CreateSecurityGroupPoliciesRequest()
    body = {
        "SecurityGroupId": sg_id,
        "SecurityGroupPolicySet": {
            "Ingress": [
                {
                    "Protocol": "TCP",
                    "Port": str(port),
                    "CidrBlock": cidr,
                    "Action": "ACCEPT",
                    "PolicyDescription": desc,
                }
            ]
        },
    }
    req.from_json_string(json.dumps(body))
    return client.CreateSecurityGroupPolicies(req).to_json_string()


def describe_sg(cred, region: str, sg_id: str):
    prof = ClientProfile(httpProfile=HttpProfile(endpoint="vpc.tencentcloudapi.com"))
    client = vpc_client.VpcClient(cred, region, prof)
    req = vpc_models.DescribeSecurityGroupPoliciesRequest()
    req.from_json_string(json.dumps({"SecurityGroupId": sg_id}))
    return json.loads(client.DescribeSecurityGroupPolicies(req).to_json_string())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("instance_id")
    ap.add_argument("port", type=int)
    ap.add_argument("--cidr", default="0.0.0.0/0")
    ap.add_argument("--desc", default="opened by tencent_open_port.py")
    ap.add_argument(
        "--region",
        default="ap-tokyo",
        help="If wrong, script will search common regions",
    )
    args = ap.parse_args()

    cred = load_creds()

    inst = find_instance(cred, args.region, args.instance_id)
    region = args.region
    if not inst:
        print(f"not in {args.region}; searching other common regions...", file=sys.stderr)
        # Tencent's most common APAC + EU + US regions
        regions = [
            "ap-singapore", "ap-hongkong", "ap-bangkok", "ap-mumbai", "ap-seoul",
            "ap-tokyo", "ap-jakarta", "na-ashburn", "na-siliconvalley", "eu-frankfurt",
        ]
        regions = [r for r in regions if r != args.region]
        region, inst = search_regions_for_instance(cred, args.instance_id, regions)
        if not inst:
            sys.exit(f"instance {args.instance_id} not found in any tried region")

    sg_ids = inst.get("SecurityGroupIds", [])
    if not sg_ids:
        sys.exit(f"instance {args.instance_id} has no security groups attached")

    sg_id = sg_ids[0]
    print(f"region={region} instance={args.instance_id} sg={sg_id}")
    print(f"opening TCP:{args.port} from {args.cidr} ({args.desc})")

    resp = add_ingress_rule(cred, region, sg_id, args.port, args.cidr, args.desc)
    print("create response:", resp)

    print("\n--- current ingress rules ---")
    data = describe_sg(cred, region, sg_id)
    for r in data.get("SecurityGroupPolicySet", {}).get("Ingress", []):
        print(f"  {r.get('Protocol'):4} {str(r.get('Port')):8} {r.get('CidrBlock', r.get('Ipv6CidrBlock','-')):20} {r.get('Action'):8} {r.get('PolicyDescription','')}")


if __name__ == "__main__":
    main()
