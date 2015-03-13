import boto
import boto.ses
import json
import os
import stat
import subprocess
import sys
import urllib

CONFIG_BUCKET = "bounce-test-config"

# JSON file that contains information with each of the providers to be tested.
# The file is structured as follows:
# [<PROVIDER 1>, <PROVIDER 2>, etc>]
# Each PROVIDER is a dictionary with the following required keys (additional
# keys will be added and passed to jclouds through system properties):
# PROVIDER: provider: "jClouds provider name",
#           identity: "identity",
#           credential: "credential"
CONFIG_KEY = "test_creds.json"

# SSH *private* key to be used to checkout the bouncestorage repository
GITHUB_KEY = "github.key"

ROLE = "BounceVerifier"
# EC2 metadata address:
# http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
META_URL = "http://169.254.169.254/latest/meta-data/iam/"\
           "security-credentials/%s" % ROLE

BOUNCE_REPO = "git@github.com:bouncestorage/bouncestorage.git"
BOUNCE_SRC_DIR = "bouncestorage"

ACCESS_KEY_FIELD = 'AccessKeyId'
SECRET_FIELD = 'SecretAccessKey'

BLOBSTORE_PROPERTY_PREFIX = 'bounce.store.properties.2.jclouds'

SSH_KEY_NAME = "/home/admin/.ssh/id_rsa"

PACKAGES = [ 'openjdk-8-jdk git maven' ]
APT_SOURCES = "/etc/apt/sources.list"
UNSTABLE_REPO = "deb http://cloudfront.debian.net/debian unstable main"

VERIFIER_SENDER = "timuralp@bouncestorage.com"
VERIFIER_RECEPIENTS = ["timuralp@bouncestorage.com",
                       "khc@bouncestorage.com",
                       "gaul@bouncestorage.com"
                      ]

OUTPUT_LOG = '/tmp/bounce_verifier.log'

class TestException(BaseException):
    pass

class Creds(object):
    def __init__(self, key, secret, token):
        self.key = key
        self.secret = secret
        self.token = token

def execute(command):
    try:
        out = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
        print out
    except subprocess.CalledProcessError as e:
        raise TestException(e.output)

def setup_code():
    with open(APT_SOURCES) as f:
        if f.read().find("unstable") == -1:
            execute("echo \"%s\"| sudo tee -a %s" % (UNSTABLE_REPO, APT_SOURCES))

    execute("sudo apt-get update")
    execute("sudo apt-get install -y openjdk-8-jdk git maven fortune cowsay")
    if os.path.exists(BOUNCE_SRC_DIR):
        execute("cd ~/%s && git pull" % BOUNCE_SRC_DIR)
    else:
        execute("echo \"StrictHostKeyChecking\tno\" | tee -a ~/.ssh/config")
        execute("cd ~ && git clone %s %s" % (BOUNCE_REPO, BOUNCE_SRC_DIR))

def get_file_dir():
    return os.path.dirname(os.path.realpath(__file__))

def get_s3_creds():
    props = json.load(urllib.urlopen(META_URL))
    return Creds(props['AccessKeyId'], props['SecretAccessKey'], props['Token'])

def get_object(creds, object_name):
    conn = boto.connect_s3(creds.key, creds.secret, security_token = creds.token)
    security_headers = { 'x-amz-security-token': creds.token }
    bucket = conn.get_bucket(CONFIG_BUCKET, validate=False, headers=security_headers)
    return bucket.get_key(object_name, headers=security_headers)

def send_email(creds, subject, body):
    conn = boto.ses.connect_to_region("us-east-1",
                                      aws_access_key_id=creds.key,
                                      aws_secret_access_key=creds.secret,
                                      security_token=creds.token)

    conn.send_email(VERIFIER_SENDER,
                    subject,
                    body,
                    VERIFIER_RECEPIENTS
                   )

def setup_github_key(creds):
    key = get_object(creds, GITHUB_KEY)
    with open(SSH_KEY_NAME, 'w') as key_file:
        key.get_contents_to_file(key_file)
        os.chmod(SSH_KEY_NAME, stat.S_IRUSR | stat.S_IWUSR)

def get_all_blobstore_credentials(creds):
    key = get_object(creds, CONFIG_KEY)
    return json.loads(key.get_contents_as_string())

def get_java_properties(provider_details):
    return ' '.join(map(lambda pair: "-D%s.%s=%s" % (BLOBSTORE_PROPERTY_PREFIX,
                    pair[0], pair[1]), provider_details.items()))

def run_test(provider_details):
    print "Testing %s" % provider_details['provider']
    java_properties = get_java_properties(provider_details)
    command = "cd ~/%s && mvn %s test" % (BOUNCE_SRC_DIR, java_properties)
    execute(command)

def notify_failure(creds, error):
    message = "Exception message:\n%s\n" % error.message
    with open(OUTPUT_LOG) as log_file:
        message += log_file.read()
    send_email(creds, "Nightly failed!", message)

def notify_success(creds):
    message = "Success!\n%s" % subprocess.check_output(["/usr/games/cowsay",
                subprocess.check_output("/usr/games/fortune")])
    with open(OUTPUT_LOG) as log_file:
        message += log_file.read()
    send_email(creds, "Nightly passed", message)

def main():
    creds = get_s3_creds()
    log = open(OUTPUT_LOG, 'w')
    sys.stdout = log

    exception = None
    try:
        setup_github_key(creds)
        setup_code()

        all_creds = get_all_blobstore_credentials(creds)

        for provider in all_creds:
            run_test(provider)
    except TestException as e:
        exception = e

    log.close()
    notify_failure(creds, exception) if exception else notify_success(creds)

if __name__ == '__main__':
    main()
