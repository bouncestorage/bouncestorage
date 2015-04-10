import argparse
import boto.ec2
import json
import urllib

ROLE = "InstanceManager"
# EC2 metadata address:
# http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
META_URL = "http://169.254.169.254/latest/meta-data/iam/"\
           "security-credentials/%s" % ROLE
VERIFIER_INSTANCE = "bounce-verifier"
REGION = 'us-west-1'

class Creds(object):
    def __init__(self, key, secret, token):
        self.key = key
        self.secret = secret
        self.token = token

def get_aws_creds():
    props = json.load(urllib.urlopen(META_URL))
    return Creds(props['AccessKeyId'], props['SecretAccessKey'], props['Token'])

def ensure_instance_state(name, creds, stop=False):
    conn = boto.ec2.connect_to_region(REGION, aws_access_key_id=creds.key,
            aws_secret_access_key=creds.secret, security_token=creds.token)
    instances = conn.get_only_instances(filters={"tag:Name" : name})
    if not instances:
        raise RuntimeError("Instance not found!")
    instance = instances[0]
    if stop:
        conn.stop_instances(instance_ids=[instance.id])
    else:
        conn.start_instances([instance.id])

def parse_args():
    parser = argparse.ArgumentParser(description='Manage the bounce verifier')
    parser.add_argument('--stop', dest='stop', action='store_true',
        help='stop the instance')
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    creds = get_aws_creds()
    ensure_instance_state(VERIFIER_INSTANCE, creds, stop=args.stop)

if __name__ == "__main__":
    main()
