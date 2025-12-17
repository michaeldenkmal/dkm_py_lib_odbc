import os.path
from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path

@dataclass
class MenuEntry:
    dep_name: str
    idx: int


def read_requirements():
    my_file = Path(__file__)
    root_dir = my_file.parent.parent.absolute()
    requirements_file = os.path.join(root_dir, "requirements.txt")

    with open(requirements_file) as f:
        return f.readlines()


def build_deps_list() -> List[MenuEntry]:
    idx = 0
    ret = []
    requs = read_requirements()
    for req in requs:
        idx = idx + 1
        ret.append(MenuEntry(dep_name=req, idx=idx))
    return ret


# https://gist.github.com/guestl/858e882a37442316fd7cdb0ca1b8e3a5
def choose_menue(menu_list: List[MenuEntry])->Optional[MenuEntry]:
    def print_menu()->int:
        _max_idx = 0
        #print(30 * "-", "Dep aussuchen", 30 * "-")
        for _menu_elem in menu_list:
            print(f"{_menu_elem.idx} {_menu_elem.dep_name}")
            _max_idx = _menu_elem.idx

        print("q. Exit from the script ")
        print(73 * "-")
        return _max_idx

    max_idx =print_menu()  # Displays menu
    choice = input(f"Enter your choice [1-{max_idx}]: ")
    for menu_elem in menu_list:
        if str(menu_elem.idx) == choice:
            return menu_elem

    return None


def do_upgrade(menue_elem:MenuEntry):
    cmd = f".\\venv\\Scripts\\pip.exe install {menue_elem.dep_name}"
    print(cmd)
    os.system(cmd)


def do_exec():
    menu_list = build_deps_list()
    maybe_menue_elem = choose_menue(menu_list)
    if maybe_menue_elem:
        do_upgrade (maybe_menue_elem)


do_exec()