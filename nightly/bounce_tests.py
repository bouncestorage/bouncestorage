#!/usr/bin/python

import boto
import boto.ses
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import traceback
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
BOUNCE_NIGHTLY_TEST_NAME = "nightly/bounce_tests.py"

DOCKER_SWIFT_REPO = "https://github.com/ualbertalib/docker-swift"
DOCKER_SWIFT_DIR = "docker-swift"
SWIFT_DATA_DIR = "/tmp/docker-swift"

ACCESS_KEY_FIELD = 'AccessKeyId'
SECRET_FIELD = 'SecretAccessKey'

BLOBSTORE_1_PROPERTY_PREFIX = 'bounce.backend.0.jclouds'
BLOBSTORE_2_PROPERTY_PREFIX = 'bounce.backend.1.jclouds'

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
        out = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True).rstrip()
        print out
        return out
    except subprocess.CalledProcessError as e:
        raise TestException(e.output)

def git_clone(repo, directory):
    if os.path.exists(directory):
        execute("cd ~/%s && git pull" % directory)
    else:
        execute("echo \"StrictHostKeyChecking\tno\" | tee -a ~/.ssh/config")
        execute("cd ~ && git clone %s %s" % (repo, directory))

def setup_code():
    with open(APT_SOURCES) as f:
        if f.read().find("unstable") == -1:
            execute("echo \"%s\"| sudo tee -a %s" % (UNSTABLE_REPO, APT_SOURCES))

    execute("sudo apt-get update")
    execute("sudo apt-get install -y openjdk-8-jdk git maven fortune cowsay docker.io")
    git_clone(BOUNCE_REPO, BOUNCE_SRC_DIR)

def setup_swift():
    git_clone(DOCKER_SWIFT_REPO, DOCKER_SWIFT_DIR)
    execute("cd ~/%s && sudo docker build -t pbinkley/docker-swift ." % DOCKER_SWIFT_DIR)

def start_docker_swift(datadir):
    if os.path.exists(datadir):
        cwd = os.getcwd()
        execute("cd %s && sudo rm -Rf *" % datadir)
        os.chdir(cwd)
    execute("sudo mkdir -p %s" % datadir)
    container = execute("sudo docker run -d -P -v %s:/swift/nodes -t pbinkley/docker-swift" % datadir)
    port = execute("sudo docker inspect --format '{{ (index (index .NetworkSettings.Ports \"8080/tcp\") 0).HostPort }}' %s" % container)
    return (container, port)

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

def get_all_blobstore_from_argv():
    return json.load(open(sys.argv[1]))

def get_java_properties(provider_details, swift_port):
    return ' '.join(map(lambda pair: "-D%s.%s=%s" % (BLOBSTORE_2_PROPERTY_PREFIX,
                                                     pair[0], pair[1]), provider_details.items()) +
                    map(lambda key: "-D%s%s" % (BLOBSTORE_1_PROPERTY_PREFIX, key), get_swift_properties(swift_port)))

def get_swift_properties(swift_port):
    return [ ".provider=openstack-swift", ".endpoint=http://127.0.0.1:%s/auth/v1.0/" % swift_port,
             ".identity=test:tester", ".credential=testing", ".keystone.credential-type=tempAuthCredentials" ]

def run_test(provider_details, swift_port, test="all"):
    print "Testing %s" % provider_details['provider']
    java_properties = get_java_properties(provider_details, swift_port)
    command = "/usr/bin/mvn %s" % (java_properties)
    if test != "all":
        command += " -Dtest=" + test
    command += " test"
    print(command)
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

def hash_file(filename):
    with open(filename, 'rb') as f:
        return hashlib.md5(f.read()).digest()

def maybe_update(log):
    updated_file = os.path.join(os.getcwd(), BOUNCE_SRC_DIR,
        BOUNCE_NIGHTLY_TEST_NAME)
    current_file = sys.argv[0]
    if hash_file(current_file) != hash_file(updated_file):
        print "Updating the test file"
        shutil.copyfile(updated_file, current_file)
        log.close()
        os.execlp("env", "env", "python", current_file)

def main():
    ec2 = len(sys.argv) == 1
    test = "all"

    if ec2:
        log = open(OUTPUT_LOG, 'w')
        sys.stdout = log
        creds = get_s3_creds()

    saio_near_container = None
    saio_far_container = None
    exception = None
    try:
        if ec2:
            setup_github_key(creds)
            setup_code()
            maybe_update(log)

            all_creds = get_all_blobstore_credentials(creds)
            os.chdir34(BOUNCE_SRC_DIR)
            setup_swift()
        else:
            all_creds = get_all_blobstore_from_argv()
            if len(sys.argv) > 2:
                test = sys.argv[2]

        saio_near_container, swift_near_port = start_docker_swift(SWIFT_DATA_DIR + "-near")
        saio_far_container, swift_far_port = start_docker_swift(SWIFT_DATA_DIR + "-far")
        all_creds += [ { "provider" : "openstack-swift",
                         "identity" : "test:tester",
                         "credential" : "testing",
                         "keystone.credential-type" : "tempAuthCredentials",
                         "endpoint" : "http://127.0.0.1:%s/auth/v1.0/" % swift_far_port } ]

        os.chdir("bounce")
        for provider in all_creds:
            run_test(provider, swift_near_port, test)
    except:
        exception = sys.exc_info()[0]
        if not ec2:
            traceback.print_exc()

    if saio_near_container is not None:
        execute("sudo docker kill %s" % saio_near_container)
        execute("sudo docker rm %s" % saio_near_container)
    if saio_far_container is not None:
        execute("sudo docker kill %s" % saio_far_container)
        execute("sudo docker rm %s" % saio_far_container)

    if ec2:
        log.close()
        notify_failure(creds, traceback.format_exc()) if exception else notify_success(creds)

if __name__ == '__main__':
    main()
