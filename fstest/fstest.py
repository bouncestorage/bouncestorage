import hashlib
import json
import os
import os.path
import random
import stat
import string
import subprocess
import swiftclient
import time
import unittest
import urllib
import urllib2

CHUNK_SIZE = 256 * 1024 * 1024
TEST_CONFIG_ENV = 'FSTEST_CONFIG'

DEFAULT_TIMEOUT = 30

# Fields in the configuration file
SHARE_PROPERTY = 'share'
SWIFT_ENDPOINT = 'endpoint'
SWIFT_USER = 'user'
SWIFT_PASSWORD = 'password'
SWIFT_CONTAINER = 'container'
MOUNT_POINT = 'mount-point'
BOUNCE_API = 'bounce-api'
USE_BOUNCE = 'use-bounce'


class TestFileSystem(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config_file = os.getenv(TEST_CONFIG_ENV)
        if not config_file:
            raise RuntimeError('%s is not defined' % TEST_CONFIG_ENV)
        cls._config = json.load(open(config_file))
        fields = [SHARE_PROPERTY, SWIFT_ENDPOINT, SWIFT_USER, SWIFT_PASSWORD]
        for f in fields:
            if f not in fields:
                raise RuntimeError("%s not defined in the configuration" % f)

    @classmethod
    def get_config(cls):
        return cls._config

    def setUp(self):
        self._config = TestFileSystem.get_config()
        self.mount_share()

    def tearDown(self):
        if self.mounted:
            self.umount_share()

    def get_random_string(self):
        char_set = string.ascii_lowercase + string.digits
        return ''.join(random.choice(char_set) for _ in range(8))

    def mount_share(self):
        share = self._config[SHARE_PROPERTY]
        subprocess.check_call(['sudo', 'mount', '-t', 'nfs', share,
                               self._config[MOUNT_POINT]])
        self.mounted = True

    def umount_share(self):
        subprocess.check_call(['sudo', 'umount', self._config[MOUNT_POINT]])
        self.mounted = False

    def write_file(self, name, size):
        key = os.urandom(16).encode('hex')
        count = size/(1024*1024)
        openssl_cmd = ('openssl enc -rc4 -K %s -bufsize 65536' % key).split()
        input_cmd = ('dd count=' + str(count) + ' bs=1M if=/dev/zero').split()
        output_cmd = ('dd ibs=65536 obs=1M of=' + name).split()
        dd_in = subprocess.Popen(input_cmd, stdout=subprocess.PIPE)
        encrypt = subprocess.Popen(openssl_cmd, stdout=subprocess.PIPE,
                                   stdin=dd_in.stdout)
        dd_out = subprocess.Popen(output_cmd, stdin=encrypt.stdout,
                                  stdout=subprocess.PIPE)
        dd_in.stdout.close()
        print dd_out.communicate()[0]
        self.assertTrue(os.path.isfile(name))
        self.assertEqual(os.stat(name).st_size, size)

    def swift_connect(self):
        authurl = self._config[SWIFT_ENDPOINT]
        swift_user = self._config[SWIFT_USER]
        swift_password = self._config[SWIFT_PASSWORD]
        return swiftclient.client.Connection(authurl=authurl,
                                             user=swift_user,
                                             key=swift_password)

    def wait_for_object(self, object_name, timeout=None):
        def _test_object(container, object_name):
            conn = self.swift_connect()
            try:
                meta = conn.head_object(container, object_name)
                return True
            except swiftclient.exceptions.ClientException as e:
                return False

        self.wait_for(_test_object, timeout, self._config[SWIFT_CONTAINER],
                      object_name)
        self.run_bounce(timeout)

    def head_object(self, object_name):
        container = self._config[SWIFT_CONTAINER]
        return self.swift_connect().head_object(container, object_name)

    def hash_file(self, full_path):
        md5 = hashlib.md5()
        chunk = 4*1024*1024
        print("hashing %s" % full_path)
        with open(full_path) as f:
            while True:
                data = f.read(chunk)
                md5.update(data)
                if len(data) < chunk:
                    break
        return md5.hexdigest()

    def wait_for(self, condition, timeout=None, *args):
        start = time.time()
        while True:
            if condition(*args):
                break
            time.sleep(0.5)
            if timeout and time.time() - start > timeout:
                break

    def run_bounce(self, timeout):
        def _wait_for_bounce(url):
            result = json.load(urllib2.urlopen(url))
            return result['endTime'] != None

        if self._config[USE_BOUNCE] != 'true':
            return

        container = self._config[SWIFT_CONTAINER]
        param = json.dumps({'name': container})
        url = '%s/bounce/%s' % (self._config[BOUNCE_API], container)
        req = urllib2.Request(url, param, {'Content-Type': 'application/json'})
        urllib2.urlopen(req).read()
        self.wait_for(_wait_for_bounce, timeout, url)

    def test_mkdir(self):
        dir_name = self.get_random_string()
        dir_path = self._config[MOUNT_POINT] + os.sep + dir_name
        os.mkdir(dir_path)
        self.umount_share()
        self.wait_for_object(dir_name)

        self.mount_share()
        self.assertTrue(os.path.isdir(dir_path))
        os.rmdir(dir_path)
        self.umount_share()

        self.mount_share()
        self.assertFalse(os.path.isdir(dir_path))

    def _write_file_test(self, min_size, max_size):
        file_name = self.get_random_string()
        full_path = os.sep.join([self._config[MOUNT_POINT], file_name])
        file_size = random.randint(min_size, max_size)*1024*1024

        self.write_file(full_path, file_size)
        self.umount_share()

        self.wait_for_object(file_name)
        head_result = self.head_object(file_name)
        self.assertEqual(int(head_result['content-length']), file_size)

        self.mount_share()
        os.remove(full_path)
        self.umount_share()
        self.mount_share()
        self.assertFalse(os.path.isfile(full_path))

    def test_large_file(self):
        chunk_size_mb = CHUNK_SIZE/(1024*1024)
        self._write_file_test(chunk_size_mb + 1, chunk_size_mb + 100)

    def test_file(self):
        self._write_file_test(1, 100)

    def test_chmod(self):
        file_name = self.get_random_string()
        full_path = os.sep.join([self._config[MOUNT_POINT], file_name])
        file_size = random.randint(1, 100) * 1024*1024
        self.write_file(full_path, file_size)
        self.umount_share()

        self.mount_share()
        new_mode = stat.S_IRUSR
        os.chmod(full_path, new_mode)
        self.assertEqual(stat.S_IMODE(os.stat(full_path).st_mode), new_mode)

        self.umount_share()
        self.mount_share()
        self.assertEqual(stat.S_IMODE(os.stat(full_path).st_mode), new_mode)

    def _overwrite_test(self, min_size, max_size):
        def _test_object_size(self, name, size):
            try:
                meta = self.head_object(name)
                return int(meta['content-length']) == size
            except swiftclient.client.ClientException as e:
                return False

        file_name = self.get_random_string()
        full_path = os.sep.join([self._config[MOUNT_POINT], file_name])
        file_size = random.randint(min_size, max_size)*1024*1024
        self.write_file(full_path, file_size)
        md5 = self.hash_file(full_path)
        self.umount_share()
        self.wait_for_object(file_name)

        self.mount_share()
        new_size = random.randint(min_size, max_size)*1024*1024
        self.write_file(full_path, new_size)
        new_md5 = self.hash_file(full_path)
        self.umount_share()

        self.wait_for(_test_object_size, DEFAULT_TIMEOUT, self, file_name,
                      new_size)
        self.mount_share()
        self.assertEqual(os.stat(full_path).st_size, new_size)
        self.assertEqual(self.hash_file(full_path), new_md5)
        os.remove(full_path)

    def test_overwrite(self):
        self._overwrite_test(1, 100)

    def test_overwrite_large_file(self):
        chunk_size_mb = CHUNK_SIZE/(1024*1024)
        self._overwrite_test(chunk_size_mb + 1, chunk_size_mb + 100)


if __name__ == '__main__':
    unittest.main()
