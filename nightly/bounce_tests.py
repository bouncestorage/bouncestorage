#!/usr/bin/python

import boto
import boto.ses
import email.mime.application
import email.mime.multipart
import email.mime.text
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
SUREFIRE_DIR = os.path.join(BOUNCE_SRC_DIR, 'bounce', 'target')
SUREFIRE_DIR_NAME = 'surefire-reports'
SUREFIRE_ARCHIVE = '/tmp/surefire.tar'
COMPRESSED_ARCHIVE = SUREFIRE_ARCHIVE + '.gz'
BOUNCE_NIGHTLY_TEST_NAME = "nightly/bounce_tests.py"

DOCKER_SWIFT_REPO = "https://github.com/kahing/docker-swift"
DOCKER_SWIFT_DIR = "docker-swift"
SWIFT_DATA_DIR = "/tmp/docker-swift"

ACCESS_KEY_FIELD = 'AccessKeyId'
SECRET_FIELD = 'SecretAccessKey'

BLOBSTORE_1_PROPERTY_PREFIX = 'bounce.backend.0.jclouds'
BLOBSTORE_2_PROPERTY_PREFIX = 'bounce.backend.1.jclouds'

SSH_KEY_NAME = "/home/admin/.ssh/id_rsa"

PACKAGES = [ 'openjdk-8-jdk git fortune cowsay docker.io' ]
APT_SOURCES = "/etc/apt/sources.list"
UNSTABLE_REPO = "deb http://cloudfront.debian.net/debian unstable main"

VERIFIER_SENDER = "timuralp@bouncestorage.com"
VERIFIER_RECIPIENTS = ["timuralp@bouncestorage.com",
                       "khc@bouncestorage.com",
                       "gaul@bouncestorage.com"
                      ]

OUTPUT_LOG = '/tmp/bounce_verifier.log'

#JAVA_PROPERTIES = [ '-DLOG_LEVEL=info' ]
JAVA_PROPERTIES = [  ]

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
    target_dir = os.path.join(os.environ['HOME'], directory)
    if os.path.exists(target_dir):
        execute("cd %s && git pull" % target_dir)
    else:
        execute("echo \"StrictHostKeyChecking\tno\" | tee -a ~/.ssh/config")
        execute("cd %s && git clone %s %s" % (os.environ['HOME'], repo, directory))

def git_update_submodule(directory):
    target_dir = os.path.join(os.environ['HOME'], directory)
    execute("cd %s && git submodule init" % target_dir)
    execute("cd %s && git submodule update" % target_dir)

def setup_code():
    with open(APT_SOURCES) as f:
        if f.read().find("unstable") == -1:
            execute("echo \"%s\"| sudo tee -a %s" % (UNSTABLE_REPO, APT_SOURCES))

    execute("sudo apt-get update")
    execute("sudo apt-get install -y " + " ".join(PACKAGES))
    git_clone(BOUNCE_REPO, BOUNCE_SRC_DIR)
    git_update_submodule(BOUNCE_SRC_DIR)
    target_dir = os.path.join(os.environ['HOME'], BOUNCE_SRC_DIR)
    execute("cd %s && mvn install -DskipTests=true" % target_dir)

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

def remove_reports():
    target_dir = os.path.join(os.environ['HOME'], SUREFIRE_DIR)
    execute("rm -f %s" % (SUREFIRE_ARCHIVE))
    execute("cd %s && rm -rf %s.*" % (target_dir, SUREFIRE_DIR_NAME))

def archive_surefire(provider):
    target_dir = os.path.join(os.environ['HOME'], SUREFIRE_DIR)
    updated_name = SUREFIRE_DIR_NAME + "." + provider
    execute("cd %s && mv %s %s" % (target_dir, SUREFIRE_DIR_NAME, updated_name))
    execute("tar -C %s -rvf %s %s" % (target_dir, SUREFIRE_ARCHIVE,
            updated_name))

def append_file_to_archive(path):
    execute("tar -rvf %s %s" % (SUREFIRE_ARCHIVE, path))

def compress_archive():
    execute("cd %s && gzip -f %s" % (os.path.dirname(SUREFIRE_ARCHIVE),
        SUREFIRE_ARCHIVE))

def send_email(creds, subject, body, attachment = None):
    conn = boto.ses.connect_to_region("us-east-1",
                                      aws_access_key_id=creds.key,
                                      aws_secret_access_key=creds.secret,
                                      security_token=creds.token)

    if attachment:
        msg = email.mime.multipart.MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = VERIFIER_SENDER
        msg['To'] = ', '.join(VERIFIER_RECIPIENTS)
        msg.preamble = 'Multipart test results.\n'
        msg.attach(email.mime.text.MIMEText(body))
        mimeAttachment = email.mime.application.MIMEApplication(open(attachment,
                'rb').read())
        mimeAttachment.add_header('Content-Disposition', 'attachment',
                filename=os.path.basename(attachment))
        msg.attach(mimeAttachment)
        conn.send_raw_email(msg.as_string(), source = VERIFIER_SENDER,
                destinations = VERIFIER_RECIPIENTS)
    else:
        conn.send_email(VERIFIER_SENDER,
                        subject,
                        body,
                        VERIFIER_RECIPIENTS
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
    blobstore_2 = map(lambda pair: "-D%s.%s=%s" % (BLOBSTORE_2_PROPERTY_PREFIX,
            pair[0], pair[1]), provider_details.items())
    blobstore_1 = map(lambda key: "-D%s%s" % (BLOBSTORE_1_PROPERTY_PREFIX, key),
            get_swift_properties(swift_port))
    return ' '.join(blobstore_2 + blobstore_1 + JAVA_PROPERTIES)

def get_swift_properties(swift_port):
    return [ ".provider=openstack-swift", ".endpoint=http://127.0.0.1:%s/auth/v1.0/" % swift_port,
             ".identity=test:tester", ".credential=testing", ".keystone.credential-type=tempAuthCredentials" ]

def run_bounce_tests(provider_details, swift_port):
    java_properties = get_java_properties(provider_details, swift_port)
    os.environ['BOUNCE_OPTS'] = java_properties + " -Dbounce.backends=0,1"
    command = "src/test/resources/run_proxy_tests.sh"
    print(command)
    execute(command)

def run_test(provider_details, swift_port, test="all"):
    print "Testing %s" % provider_details['provider']
    java_properties = get_java_properties(provider_details, swift_port)
    command = "/usr/bin/mvn %s" % (java_properties)
    if test != "all":
        command += " -Dtest=" + test
    command += " test"
    print(command)
    execute(command)
    run_bounce_tests(provider_details, swift_port)

def notify_failure(creds, error, backtrace):
    message = "Exception message:\n%s\n" % error.message
    message += backtrace + "\n"
    send_email(creds, "Nightly failed!", message, COMPRESSED_ARCHIVE)

def notify_success(creds):
    message = "Success!\n%s" % subprocess.check_output(["/usr/games/cowsay",
                subprocess.check_output("/usr/games/fortune")])
    send_email(creds, "Nightly passed", message, COMPRESSED_ARCHIVE)

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
    #os.environ['LOG_LEVEL'] = 'info'

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
            os.chdir(BOUNCE_SRC_DIR)
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
        if ec2:
            remove_reports()
        for provider in all_creds:
            try:
                run_test(provider, swift_near_port, test)
            finally:
                if ec2:
                    archive_surefire(provider["provider"])
    except:
        exception = sys.exc_info()[1]
        if not ec2:
            traceback.print_exc()

    if saio_near_container is not None:
        execute("sudo docker kill %s" % saio_near_container)
        execute("sudo docker rm %s" % saio_near_container)
    if saio_far_container is not None:
        execute("sudo docker kill %s" % saio_far_container)
        execute("sudo docker rm %s" % saio_far_container)

    if ec2:
        sys.stdout.flush()
        log.close()
        sys.stdout = open(os.devnull, 'w')
        append_file_to_archive(OUTPUT_LOG)
        compress_archive()
        notify_failure(creds, exception, traceback.format_exc()) if exception else notify_success(creds)
        execute("sudo poweroff")
    if exception:
        sys.exit(1)

if __name__ == '__main__':
    main()
