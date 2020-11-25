import subprocess as sp
from multiprocessing import Pool
import os
from subprocess import STDOUT, SubprocessError 
from config import LOGIN, Color
import re


def clean(machine_name: str) -> None:
    """ Fonction permettant de nettoyer une machine en suprimant le programme slave """
    try:
        r = sp.run(f"ssh {LOGIN}@{machine_name.rstrip()} rm -r /tmp/fpont/".split(), check=True, capture_output=True)
    except sp.CalledProcessError as e:
        if len(re.findall("Aucun fichier", e.stderr.decode())) or len(re.findall("No such file", e.stderr.decode())):
            print(f"Nettoyage de la machine {machine_name}: {Color.BLUE}INEXISTANT{Color.END}")
        else:
            print(f"Nettoyage de la machine {machine_name}: {Color.RED}FAIL{Color.END}")
    else:
        print(f"Nettoyage de la machine {machine_name}: {Color.GREEN}OK{Color.END}")


def clean_cluster():    
    """ Appelle la fonction 'clean' sur tout le cluster """
    with Pool(os.cpu_count()) as pool:
        with open('machines.txt', 'r') as f, open('master.txt', 'r') as m:
            names = [name.rstrip() for name in f.readlines()]
            names += [m.readlines()[0]]
            pool.map(clean, names)
            pool.close()
            pool.join()


if __name__ == "__main__":
    os.chdir(f"/tmp/{LOGIN}/")
    clean_cluster()