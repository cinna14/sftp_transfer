import traceback
from paramiko import RSAKey, Transport, SFTPClient
import os
import io


def is_empty(string):
    if string is None:
        return True
    string = string.strip()
    if "" == string:
        return True
    return False


class BadConfigError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SFTPConfig:

    def __init__(self, hostname, port, host_public_key, username, password, private_key, destination_directory,
                 destination_file_name):
        self.hostname = hostname
        self.port = port
        self.host_public_key = host_public_key
        self.username = username
        self.password = password
        self.private_key = private_key
        self.destination_directory = destination_directory
        self.destination_file_name = destination_file_name


class S3ToSFTPTransferHandler:

    def __init__(self):
        self.transfer_composition = SFTPConfig(
            's-e4157ea38a5143e7a.server.transfer.eu-west-1.amazonaws.com',
            22,
            '',
            'unabstracted-user',
            '',
            """-----BEGIN OPENSSH PRIVATE KEY-----
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
-----END OPENSSH PRIVATE KEY-----""",
            '/unabstracted-bucket',
            'destination.txt'
        )

        self.sftp_client = self.__create_sftp_client()

    def transfer(self):
        if not is_empty(self.transfer_composition.destination_directory):
            self.sftp_client.chdir(self.transfer_composition.destination_directory)

        local_file = 'README.md'
        with open(local_file, 'rb') as f1:
            with self.sftp_client.file(self.transfer_composition.destination_file_name, 'wb') as sftp_file:
                # IMP! enabling pipelining speeds up transfer
                sftp_file.set_pipelined(True)
                sftp_file.write(f1.read())

        print(self.sftp_client.listdir(self.transfer_composition.destination_directory))
        print(f"File Transferred : from {os.getcwd()}/{local_file} to "
              f"{self.transfer_composition.destination_directory}/{self.transfer_composition.destination_file_name}")

    def __get_client_private_key(self):
        if not is_empty(self.transfer_composition.private_key):
            try:
                return RSAKey.from_private_key(io.StringIO(self.transfer_composition.private_key))
            except Exception as e:
                traceback.print_exc()
                raise BadConfigError('unable to parse client private key')

    def __create_sftp_client(self):
        transport = Transport(self.transfer_composition.hostname, self.transfer_composition.port)
        transport.use_compression(True)

        try:
            transport.connect(hostkey=self.transfer_composition.host_public_key or None,
                              username=self.transfer_composition.username,
                              password=self.transfer_composition.password or None,
                              pkey=self.__get_client_private_key())
        except Exception as e:
            traceback.print_exc()
            raise e
        return SFTPClient.from_transport(transport)


if __name__ == "__main__":
    transfer_obj = S3ToSFTPTransferHandler()
    transfer_obj.transfer()