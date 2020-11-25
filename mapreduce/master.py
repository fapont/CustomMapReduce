from itertools import cycle
import os
import subprocess
from multiprocessing import Pool, Lock
from subprocess import CalledProcessError, TimeoutExpired
import time
from pathlib import Path
from config import LOGIN, Color
import shutil

def timeit(method):
    """ Méthoder permettant de définir le décorateur @timeit permettant de chronométrer une fonction """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()  
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % (method.__name__.upper(), (te - ts) * 1000))
        return result
    return timed


def init(l):
    """ Fonction permettant d'initialiser un Locker pour le multiprocessing """
    global lock
    lock = l


def run_map(machine_name: str) -> None:
    """ Méthode permettant de lancer la phase de map sur une machine """
    try:
        subprocess.run(f"ssh {LOGIN}@{machine_name.rstrip()} 'python3 /tmp/{LOGIN}/slave.py 0 /tmp/{LOGIN}'",\
             shell=True, check=True, timeout=100)
    except (CalledProcessError, TimeoutExpired):
        print(f"Exécution du MAP sur {machine_name}: {Color.RED}FAIL{Color.END}")
        # Process parallélisé donc on lock le fichier où écrire
        lock.acquire()
        with open("map.error", "a") as f:
            f.write(f"{machine_name}\n")
        lock.release()  


@timeit
def run_map_on_cluster():
    """ Méthode permettant de lancer la fonction 'run_map' sur tout le cluster de manière parallélisée """
    l = Lock()
    pool = Pool(os.cpu_count(), initializer=init, initargs=(l,))
    with open('used_machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map_async(run_map, names)
        # Attente de la fin d'exécution
        pool.close()
        pool.join()
    print("MAP FINISHED")


def run_shuffle(machine_name: str) -> None:
    """ Méthode permettant de lancer la phase de shuffle sur une machine """
    try:
        subprocess.run(f"ssh {LOGIN}@{machine_name.rstrip()} 'python3 /tmp/{LOGIN}/slave.py 1 /tmp/{LOGIN}'", \
            shell=True, check=True, timeout=600)
    except (CalledProcessError, TimeoutExpired):
        print(f"Exécution du SHUFFLE sur {machine_name}: {Color.RED}FAIL{Color.END}")   
        # Process parallélisé donc on lock le fichier où écrire
        lock.acquire()
        with open("shuffle.error", "a") as f:
            f.write(f"{machine_name}\n")
        lock.release()  


@timeit
def run_shuffle_on_cluster():
    """ Méthode permettant de lancer la fonction 'run_shuffle' sur tout le cluster de manière parallélisée """
    l = Lock()
    pool = Pool(os.cpu_count(), initializer=init, initargs=(l,))
    with open('used_machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map(run_shuffle, names)
        # Attente de la fin d'exécution
        pool.close()
        pool.join()
    print("SHUFFLE FINISHED")


def run_reduce(machine_name: str) -> None:
    """ Méthode permettant de lancer la phase de reduce sur une machine """
    try:
        subprocess.run(f"ssh {LOGIN}@{machine_name} 'python3 /tmp/{LOGIN}/slave.py 2 /tmp/{LOGIN}'", \
            shell=True, check=True, timeout=100)
    except (CalledProcessError, TimeoutExpired):
        print(f"Exécution du REDUCE sur {machine_name}: {Color.RED}FAIL{Color.END}")   
        # Process parallélisé donc on lock le fichier où écrire
        lock.acquire()
        with open("reduce.error", "a") as f:
            f.write(f"{machine_name}\n")
        lock.release()  


@timeit
def run_reduce_on_cluster():
    """ Méthode permettant de lancer la fonction 'run_reduce' sur tout le cluster de manière parallélisée """
    l = Lock()
    pool = Pool(os.cpu_count(), initializer=init, initargs=(l,))
    with open('used_machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map(run_reduce, names)
        # Attente de la fin d'exécution
        pool.close()
        pool.join()
    print("REDUCE FINISHED")


@timeit
def split_data(path: Path, n: int) -> None:
    """ Méthode permettant de créer des splits à partir d'une données d'entrée"""
    assert path.exists(), f"{Color.RED}ERROR{Color.END}: Données non existante"
    # Comptage du nombre de lignes du fichier
    r = subprocess.run(f"wc -l {path}".split(), capture_output=True)
    n_lines = int(r.stdout.decode().split()[0])

    # Exécution du script de split
    Path().cwd().joinpath('split').mkdir(exist_ok=True)
    split_path = Path().cwd().joinpath('split')
    subprocess.run(f"split -d -e -l {n_lines // n + n_lines % n} {path} S".split(), cwd=split_path)


@timeit
def deploy() -> None:
    """ Fonction permettant de lancer le déploiement des slaves et splits"""
    subprocess.run('python3 deploy.py'.split())


def sort_output_on_cluster(n: int=10):
    """ 
        Méthode permettant de lancer la fonction 'sort_output' sur tout le cluster de manière parallélisée. Puis
        trie les valeurs sur le master.
    """
    pool = Pool(os.cpu_count())
    with open('used_machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.starmap(sort_output, list(zip(names, cycle([n]))))
        # Attente de la fin d'exécution
        pool.close()
        pool.join()    


def sort_output(machine_name: str, n: int) -> None:
    """ Méthode permettant de trier les résulats du mapReduce sur les machines distantes """
    try:
        subprocess.run(f"ssh {LOGIN}@{machine_name.rstrip()} 'python3 /tmp/{LOGIN}/slave.py 3 /tmp/{LOGIN} {n}'", \
            shell=True, check=True, timeout=100)
    except (CalledProcessError, TimeoutExpired):
        print(f"Exécution du SORT sur {machine_name}: {Color.RED}FAIL{Color.END}") 


def get_output(machine_name: str) -> None:
    """ Méthode permettant de récupérer les résultats d'une machine sur le master """
    try:
        subprocess.run(f"rsync -z {LOGIN}@{machine_name}:/tmp/{LOGIN}/reduces/output.txt temp/{machine_name}.txt".split(), check=True)
    except CalledProcessError:
        print(f"Récupération des résultats sur {machine_name}: {Color.RED}FAIL{Color.END}") 


def get_output_on_cluster() -> None:
    """ Méthode permettant de récupérer tous les résultats et de les aggréger de manière parallèle"""
    pool = Pool(os.cpu_count())
    with open('used_machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map(get_output, names)
        # Attente de la fin d'exécution
        pool.close()
        pool.join()   


def display_result(n: int):
    """ Fonction servant à trier les outputs sur les machines distantes et à ressortir n valeurs triées parmis toutes"""
    Path().cwd().joinpath("temp").mkdir(exist_ok=True)
    # Lancement des tris sur les slaves
    try:
        sort_output_on_cluster(n)
    except Exception:
        print(f"{Color.RED}Impossible de récupérer les données triées{Color.END}")
        os.removedirs("temp")
    # Récupération des n plus grandes valeurs de chaque slave
    get_output_on_cluster()
    # Lecture des fichiers importés et tri pour récupérer les n plus grandes valeurs
    counts = []
    for file in Path.cwd().joinpath("temp").iterdir():
        with open(file, 'r') as f:
            for line in f.readlines():
                counts.append(line.split())
    counts = sorted(counts, key=lambda x: int(x[1]), reverse=True)
    # Affichage des résultats
    print(f"{Color.BLUE}================ Résultats ================{Color.END}")
    for elem in counts[:n]:
        print(f"{elem[0]} {elem[1]}")
    # Suppression du dossier temporaire
    shutil.rmtree("temp")

@timeit
def main():
    # On split les données en n_machine
    os.chdir(f"/tmp/{LOGIN}/")
    try:
        r = subprocess.run(f"wc -l /tmp/{LOGIN}/machines.txt".split(), capture_output=True, check=True)
    except CalledProcessError:
        print(f"Lecture du fichier 'machines.txt': {Color.RED}FAIL{Color.END}")   
        raise
    n_machine = int(r.stdout.decode().split()[0])
    split_data(Path.cwd().joinpath("data/input.txt"), n_machine)
    deploy() # Créé le fichier "used_machines"
    run_map_on_cluster()
    run_shuffle_on_cluster()
    run_reduce_on_cluster()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"{Color.RED}Impossible de finir l'exécution{Color.END}")
    else:
        display_result(10)
