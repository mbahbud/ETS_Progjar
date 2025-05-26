import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):
        os.chdir('files/')

    def list(self, params=[]):
        try:
            filelist = glob('*.*')
            return dict(status='OK', data=filelist)
        except Exception as e:
            return dict(status='ERROR', data=str(e))

    def get(self, params=[]):
        try:
            filename = params[0]
            if filename == '':
                return None
            fp = open(f"{filename}", 'rb')
            isifile = base64.b64encode(fp.read()).decode()
            return dict(status='OK', data_namafile=filename, data_file=isifile)
        except Exception as e:
            return dict(status='ERROR', data=str(e))

    def upload(self, params=[]):
        try:
            filename = params[0]
            filedata = base64.b64decode(params[1])
            with open(filename, 'wb') as fp:
                fp.write(filedata)
            return dict(status='OK', data=f'File {filename} berhasil diupload')
        except Exception as e:
            return dict(status='ERROR', data=str(e))

    def delete(self, params=[]):
        try:
            filename = params[0]
            os.remove(filename)
            return dict(status='OK', data=f'File {filename} berhasil dihapus')
        except Exception as e:
            return dict(status='ERROR', data=str(e))


if __name__ == '__main__':
    f = FileInterface()
    print(f.list())
    print(f.get(['pokijan.jpg']))