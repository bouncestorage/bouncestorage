import json
import os
import os.path
import random
import string
import subprocess
import swiftclient
import time
import unittest

CHUNK_SIZE = 2 * 1024 * 1024 * 1024
TEST_CONFIG_ENV = 'FSTEST_CONFIG'

# Fields in the configuration file
SHARE_PROPERTY = 'share'
SWIFT_ENDPOINT = 'endpoint'
SWIFT_USER = 'user'
SWIFT_PASSWORD = 'password'
SWIFT_CONTAINER = 'container'
MOUNT_POINT = 'mount-point'


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

    def swift_connect(self):
        authurl = self._config[SWIFT_ENDPOINT]
        swift_user = self._config[SWIFT_USER]
        swift_password = self._config[SWIFT_PASSWORD]
        return swiftclient.client.Connection(authurl=authurl,
                                             user=swift_user,
                                             key=swift_password)

    def wait_for_object(self, object_name, timeout=None):
        start_time = time.time()
        while True:
            conn = self.swift_connect()
            try:
                conn.head_object(self._config[SWIFT_CONTAINER], object_name)
                break
            except swiftclient.exceptions.ClientException as e:
                if timeout and time.time() - start_time > timeout:
                    raise e
                time.sleep(0.5)

    def head_object(self, object_name):
        container = self._config[SWIFT_CONTAINER]
        return self.swift_connect().head_object(container, object_name)

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

    def test_file(self):
        file_name = self.get_random_string()
        full_path = os.sep.join([self._config[MOUNT_POINT], file_name])
        meg = 1024*1024
        file_size = random.randint(meg, 1024*meg)/(meg)*(meg)

        self.write_file(full_path, file_size)
        self.assertTrue(os.path.isfile(full_path))
        self.assertEqual(os.stat(full_path).st_size, file_size)
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
        file_name = self.get_random_string()
        full_path = os.sep.join([self._config[MOUNT_POINT], file_name])
        file_size = CHUNK_SIZE + 1024*1024
        self.write_file(full_path, file_size)
        self.assertTrue(os.path.isfile(full_path))
        self.assertEqual(os.stat(full_path).st_size, file_size)
        self.umount_share()

        self.wait_for_object(file_name)
        head_result = self.head_object(file_name)
        self.assertEqual(int(head_result['content-length']), file_size)

        self.mount_share()
        self.assertTrue(os.path.isfile(full_path))
        self.assertEqual(os.stat(full_path).st_size, file_size)

        os.remove(full_path)
        self.umount_share()
        self.mount_share()
        self.assertFalse(os.path.isfile(file_name))


if __name__ == '__main__':
    unittest.main()
