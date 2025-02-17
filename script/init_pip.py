# -*- coding:utf-8 -*-
import os
import logging

# 目录地址配置
current_dir, _ = os.path.split(__file__)
CURRENT_DIR = current_dir or os.getcwd()  # 当前目录
SOURCE_PATH = os.path.abspath(os.path.dirname(CURRENT_DIR))  # 上一层目录，认为是源目录


def read_requirements(file_path):
    with open(file_path, encoding='utf-8') as f:
        contents = f.read()
        lines = contents.splitlines()
        lines = [l.lower().replace('_', '-') for l in lines]
        return lines


def pip_requirements(file_path, bak_file):
    lines = read_requirements(file_path)
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        print(line)
        print(os.popen('pip install --force-reinstall ' + line, ).read())
    compare(lines, bak_file)


def compare(lines, bak_file):
    # 生成最新列表
    print(os.popen('pip freeze > ' + bak_file).read())
    # 对比
    r3 = read_requirements(bak_file)
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line in r3:
            print('库相同', line)
        elif '==' in line:
            print('** 需要', line)
            print(os.popen('pip install --no-dependencies --force-reinstall ' + line).read())


def pip_requirements_all(file_path):
    lines = read_requirements(file_path)
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        print(line)
        print(os.popen('pip install --no-dependencies ' + line, ).read())


if __name__ == "__main__":
    # pip_requirements(os.path.join(SOURCE_PATH, 'requirements.txt'), os.path.join(SOURCE_PATH, 'requirements_bak.txt'))
    pip_requirements_all(os.path.join(SOURCE_PATH, 'requirements.txt'))
