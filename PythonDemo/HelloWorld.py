import os


def dirlist(path):
    filelist = os.listdir(path)
    n = 0

    for filename in filelist:
        filepath = os.path.join(path, filename)
        n = filepath.count(os.path.sep)
        if os.path.isdir(filepath):
            print('  ' * (n) + '--' + filename)
            dirlist(filepath)
        else:
            print('  ' * (n) + '--' + filename)


dirlist("d:/data")