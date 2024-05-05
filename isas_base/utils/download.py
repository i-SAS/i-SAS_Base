import argparse
import os
import subprocess


def main() -> None:
    # set arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_id', type=str, default=os.getenv('DATA_ID', None))
    parser.add_argument('--data_source', type=str, default=os.getenv('DATA_SOURCE', None))
    args = parser.parse_args()
    if args.data_id is None:
        raise ValueError('data_id is not specified.')
    if args.data_source is None:
        raise ValueError('data_source is not specified.')
    # download
    download(args.data_id, args.data_source)


def download(
        data_id: str,
        data_source: str,
        ) -> None:
    download = {
        'GoogleDrive': download_from_googledrive,
        }
    if data_source not in download:
        raise ValueError('Unsupproted datasource')
    download[data_source](data_id)


def download_from_googledrive(data_id: str) -> None:
    url = 'https://drive.google.com/uc'
    file_name = '/root/datadrive/data_tmp.zip'
    code = "$(awk '/_warning_/ {print $NF}' /tmp/cookie)"
    commands = (
        f'curl -sc /tmp/cookie "{url}?export=download&id={data_id}" >  /dev/null',
        f'curl -Lb /tmp/cookie "{url}?export=download&confirm={code}&id={data_id}" -o {file_name}',
        f'unzip -oq {file_name} -d /root/datadrive',
        f'rm {file_name}',
        )
    for command in commands:
        subprocess.run(command, shell=True, check=True, encoding='utf-8')


if __name__ == '__main__':
    main()
