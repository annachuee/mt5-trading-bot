"""Provision or terminate a temporary Windows Server EC2 instance.

The script uses boto3's standard credential chain and never prints passwords,
access keys, or private key material. RDP access must be restricted to one
administrator IPv4 /32 in a preconfigured security group.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_REPORT = ROOT / "work" / "vps_provisioning_report.json"
WINDOWS_AMI_PARAMETER = "/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base"


def _aws(profile: str | None, region: str):
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
    except ImportError as exc:
        raise RuntimeError("AWS support is not installed; run: python -m pip install -r requirements-cloud.txt") from exc
    session = boto3.Session(profile_name=profile or None, region_name=region)
    try:
        session.client("sts").get_caller_identity()
    except NoCredentialsError as exc:
        raise RuntimeError(
            "No AWS credentials are available in the standard credential chain. "
            "Configure an AWS profile or workload identity, then retry."
        ) from exc
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"AWS identity check failed ({type(exc).__name__}); check profile permissions") from exc
    return session


def _validate_rdp_group(ec2: Any, group_id: str, admin_cidr: str) -> None:
    network = ipaddress.ip_network(admin_cidr, strict=False)
    if network.version != 4 or network.prefixlen != 32:
        raise ValueError("--admin-cidr must be one IPv4 address with /32, for example 203.0.113.10/32")
    response = ec2.describe_security_groups(GroupIds=[group_id])
    groups = response.get("SecurityGroups", [])
    if not groups:
        raise ValueError(f"Security group {group_id} was not found")
    allowed = False
    for permission in groups[0].get("IpPermissions", []):
        protocol = permission.get("IpProtocol")
        from_port, to_port = permission.get("FromPort"), permission.get("ToPort")
        is_rdp = (
            protocol == "tcp"
            and isinstance(from_port, int)
            and isinstance(to_port, int)
            and from_port <= 3389 <= to_port
        )
        if not is_rdp:
            raise ValueError("Security group must allow only the required RDP ingress rule")
        cidrs = {item.get("CidrIp") for item in permission.get("IpRanges", [])}
        if permission.get("Ipv6Ranges") or permission.get("PrefixListIds") or cidrs != {str(network)}:
            raise ValueError("RDP ingress must be restricted exactly to --admin-cidr; no IPv6/prefix-list access")
        allowed = True
    if not allowed:
        raise ValueError(f"Security group must contain TCP 3389 ingress from exactly {network}")


def provision(args: argparse.Namespace) -> dict[str, Any]:
    session = _aws(args.profile, args.region)
    ec2 = session.client("ec2")
    _validate_rdp_group(ec2, args.security_group_id, args.admin_cidr)

    ami_id = args.ami_id
    if not ami_id:
        ssm = session.client("ssm")
        try:
            ami_id = ssm.get_parameter(Name=WINDOWS_AMI_PARAMETER)["Parameter"]["Value"]
        except Exception as exc:
            raise RuntimeError("Could not resolve the current Windows Server 2022 AMI in this region") from exc

    response = ec2.run_instances(
        ImageId=ami_id,
        InstanceType=args.instance_type,
        MinCount=1,
        MaxCount=1,
        KeyName=args.key_name,
        NetworkInterfaces=[{
            "DeviceIndex": 0,
            "SubnetId": args.subnet_id,
            "Groups": [args.security_group_id],
            "AssociatePublicIpAddress": True,
        }],
        BlockDeviceMappings=[{
            "DeviceName": "/dev/sda1",
            "Ebs": {"VolumeSize": args.volume_gib, "VolumeType": "gp3", "Encrypted": True,
                    "DeleteOnTermination": True},
        }],
        MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Name", "Value": args.name},
                {"Key": "Purpose", "Value": "temporary-mt5-dry-run"},
                {"Key": "AutoStopAfterHours", "Value": str(args.ttl_hours)},
                {"Key": "CreatedBy", "Value": "spin_up_vps.py"},
            ],
        }],
        ClientToken=str(uuid.uuid4()),
    )
    instance = response["Instances"][0]
    instance_id = instance["InstanceId"]
    print(f"Instance requested: {instance_id} ({args.region}, {args.instance_type})", flush=True)
    try:
        waiter = ec2.get_waiter("instance_running")
        waiter.wait(InstanceIds=[instance_id], WaiterConfig={"Delay": 15, "MaxAttempts": 40})
        deadline = time.monotonic() + 120
        public_ip = None
        while time.monotonic() < deadline:
            details = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]
            public_ip = details.get("PublicIpAddress")
            if public_ip:
                break
            time.sleep(5)
        if not public_ip:
            raise RuntimeError("Instance became active but did not receive a public IP")
    except Exception:
        print(f"Provisioning did not finish; instance {instance_id} may still exist. Terminate it with --terminate {instance_id}.", file=sys.stderr)
        raise

    report = {
        "provider": "aws",
        "region": args.region,
        "instance_id": instance_id,
        "public_ip": public_ip,
        "os_image_id": ami_id,
        "os": "Windows Server 2022 Base",
        "instance_type": args.instance_type,
        "rdp_username": "Administrator",
        "key_pair_name": args.key_name,
        "rdp_source_cidr": args.admin_cidr,
        "temporary_ttl_hours_tag": args.ttl_hours,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "secret_material_logged": False,
        "note": "No password or private key is stored here. Retrieve the Windows Administrator password through AWS using the existing key pair.",
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    args.report.chmod(0o600)
    print(f"Public IP: {public_ip}")
    print("RDP user: Administrator")
    print(f"Key pair: {args.key_name} (private key is not printed or copied)")
    print(f"Report: {args.report}")
    print(f"To terminate: python spin_up_vps.py --terminate {instance_id} --region {args.region}")
    return report


def terminate(instance_id: str, region: str, profile: str | None) -> None:
    session = _aws(profile, region)
    try:
        session.client("ec2").terminate_instances(InstanceIds=[instance_id])
    except Exception as exc:
        raise RuntimeError(f"Could not request termination of {instance_id} ({type(exc).__name__})") from exc
    print(f"Termination requested for {instance_id} in {region}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="eu-west-2", help="AWS region; default eu-west-2 (London)")
    parser.add_argument("--profile", help="Optional AWS profile name")
    parser.add_argument("--terminate", metavar="INSTANCE_ID", help="Terminate an instance instead of creating one")
    parser.add_argument("--instance-type", default="t3.medium")
    parser.add_argument("--name", default="temporary-mt5-dry-run")
    parser.add_argument("--ttl-hours", type=int, default=4, help="Informational TTL tag; arrange a cloud cleanup rule")
    parser.add_argument("--volume-gib", type=int, default=50)
    parser.add_argument("--ami-id", help="Optional pinned Windows Server 2022 AMI ID")
    parser.add_argument("--key-name", help="Existing EC2 key pair name")
    parser.add_argument("--subnet-id", help="Public subnet ID in the selected region")
    parser.add_argument("--security-group-id", help="Security group with RDP limited to --admin-cidr")
    parser.add_argument("--admin-cidr", help="Your current public IPv4 address with /32")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    if args.terminate:
        terminate(args.terminate, args.region, args.profile)
        return 0
    missing = [flag for flag, value in (
        ("--key-name", args.key_name), ("--subnet-id", args.subnet_id),
        ("--security-group-id", args.security_group_id), ("--admin-cidr", args.admin_cidr),
    ) if not value]
    if missing:
        parser.error("for create, provide " + ", ".join(missing))
    if args.volume_gib < 30 or args.ttl_hours < 1:
        parser.error("--volume-gib must be at least 30 and --ttl-hours must be positive")
    try:
        provision(args)
    except RuntimeError as exc:
        print(f"Provisioning stopped: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
