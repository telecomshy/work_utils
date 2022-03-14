from ftplib import FTP
from pathlib import Path


class FtpClient:
    """
    扩展原生FTP，增加一些方法
    """

    def __init__(self, *args, **kwargs):
        self.ftp = FTP(*args, **kwargs)

    def close(self, *args):
        self.ftp.__exit__(*args)

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args):
        self.close(*args)

    def upload(self, local_filepath, ftp_dir='.', ftp_filename=None, delete_file=False):
        """
        上传文件

        :param ftp_filename: 上传到ftp的文件名称，默认和本地文件同名
        :param delete_file: 上传完毕后是否删除本地文件
        :param local_filepath:  本地文件的路径
        :param ftp_dir: 上传值ftp的目录
        :return: None
        """
        local_filepath = Path(local_filepath)

        # 默认情况下，如果路径是一个目录，则抛出PermissionError，这里让提示更明确
        if not local_filepath.is_file():
            raise TypeError('local_filepath must be a file.')

        if ftp_filename is None:
            ftp_filename = local_filepath.name

        with open(local_filepath, 'rb') as f:
            self.cwd(ftp_dir)
            self.storbinary(f'STOR {ftp_filename}', f)

        if delete_file:
            local_filepath.unlink(missing_ok=True)

    def download(self, ftp_filename, local_path, local_filename=None):
        """
        下载文件

        :param ftp_filename: ftp服务器上的文件名称，其它位置参数为本地路径
        :type ftp_filename: str list
        :param local_path: 本地保存地址
        :param local_filename: 本地文件名，默认与FTP文件同名
        :return: None
        """
        if local_filename is None:
            local_filename = ftp_filename

        local_path = Path(local_path)

        if not local_path.is_dir():
            raise TypeError('local_path mush be a dir.')

        local_path.mkdir(exist_ok=True)
        local_filepath = local_path / local_filename

        with open(local_filepath, 'wb') as f:
            self.ftp.retrbinary(f"RETR {ftp_filename}", f.write)

    def __getattr__(self, attr):
        return getattr(self.ftp, attr)
