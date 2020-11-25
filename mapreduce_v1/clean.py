import subprocess as sp
from multiprocessing import Pool
import os 


LOGIN='fpont'


def clean(machine_name: str) -> None:
    """ Fonction permettant de nettoyer une machine en suprimant le programme slave """
    try:
        sp.run(f"ssh {LOGIN}@{machine_name.rstrip()} rm -r /tmp/fpont/".split(), check=True)
        print(f"Nettoyage de la machine {machine_name}: OK")
    except sp.CalledProcessError:
        print(f"Nettoyage de la machine {machine_name}: FAIL")


def clean_cluster():
    """ Appelle la fonction 'clean' sur tout le cluster """
    assert os.path.exists('machines.txt')
    with Pool(os.cpu_count()) as pool:
        with open('machines.txt', 'r') as f:
            names = [name.rstrip() for name in f.readlines()]
            pool.map(clean, names)
            pool.close()
            pool.join()


if __name__ == "__main__":
    clean_cluster()