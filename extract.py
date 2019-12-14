import os
import rarfile


def find_rars(dir_path):
    rarname = None
    subname = None
    for child in os.listdir(dir_path):
        if 'rar' in child.lower():
            rarname = child
        elif 'sub' in child.lower():
            sub_rar = None
            for sub_child in os.listdir(child):
                if 'rar' in sub_child:
                    sub_rar = sub_child
                    break
            subname = sub_rar or child
    if rarname:
        return dir_path, rarname, subname, None


# Some idgets rar rars, with more rars on top
def recursive_unrar(path):
    dirname = os.path.dirname(path)
    with rarfile.RarFile(path) as rf:
        destroy_files = rf.volumelist()
        rf.extractall(path=dirname)
        for f in rf.infolist():
            if f.filename.endswith('rar'):
                self.recursive_unrar(os.path.join(dirname, f.filename))
    for rar_part in destroy_files:
        os.remove(os.path.join(dirname, rar_part))
