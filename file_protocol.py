import json
import logging
import shlex

from file_interface import FileInterface

class FileProtocol:
    def __init__(self):
        self.file = FileInterface()

    def proses_string(self, string_datamasuk=''):
        logging.warning(f"string diproses: {string_datamasuk}")
        c = shlex.split(string_datamasuk.lower())
        try:
            c_request = c[0].strip()
            logging.warning(f"memproses request: {c_request}")
            params = [x for x in c[1:]]
            if c_request == 'upload':
                cl = self.file.upload(params)
            elif c_request == 'delete':
                cl = self.file.delete(params)
            else:
                cl = getattr(self.file, c_request)(params)
            return json.dumps(cl)
        except Exception:
            return json.dumps(dict(status='ERROR', data='request tidak dikenali'))


if __name__ == '__main__':
    fp = FileProtocol()
    print(fp.proses_string("LIST"))
    print(fp.proses_string("GET pokijan.jpg"))