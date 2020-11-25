import subprocess as sp
from multiprocessing import Pool
import os 


LOGIN='fpont'


def clean(machine_name: str) -> None:
    """ Fonction permettant de nettoyer une machine en suprimant le programme slave """
    try:
        sp.run(f"ssh {LOGIN}@{machine_name.rstrip()} rm -r /tmp/fpont/".split(), check=True)
    except sp.CalledProcessError:
        print(f"Nettoyage de la machine {machine_name}: FAIL")
    else:
        print(f"Nettoyage de la machine {machine_name}: OK")


def clean_cluster():
    """ Appelle la fonction 'clean' sur tout le cluster """
    assert os.path.exists('machines.txt')
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