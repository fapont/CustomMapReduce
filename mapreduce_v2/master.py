import os
import subprocess
from multiprocessing import Pool
import time
from pathlib import Path



LOGIN='fpont'


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
            print('%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000))
        return result
    return timed

                   
def run_map(machine_name: str) -> None:
    """ Méthode permettant de lancer la phase de map sur une machine """
    try:
        subprocess.run(f"ssh {LOGIN}@{machine_name.rstrip()} 'python3 /tmp/{LOGIN}/slave.py 0 /tmp/{LOGIN}'", shell=True, check=True)
        #print(f"Exécution du MAP sur {machine_name}: OK")
    except subprocess.CalledProcessError:
        print(f"Exécution du MAP sur {machine_name}: FAIL")        


@timeit
def run_map_on_cluster():
    """ Méthode permettant de lancer la fonction 'run_map' sur tout le cluster de manière parallélisée """
    assert os.path.exists('machines.txt')
    pool = Pool(os.cpu_count())
    with open('machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map_async(run_map, names)
        # Attente de la fin d'exécution
        pool.close()
        pool.join()
    print("MAP FINISHED")


# Rajouter des timeout
def run_shuffle(machine_name: str) -> None:
    """ Méthode permettant de lancer la phase de shuffle sur une machine """
    try:
        subprocess.run(f"ssh {LOGIN}@{machine_name.rstrip()} 'python3 /tmp/{LOGIN}/slave.py 1 /tmp/{LOGIN}'", shell=True, check=True)
        #print(f"Exécution du SHUFFLE sur {machine_name}: OK")
    except subprocess.CalledProcessError:
        print(f"Exécution du SHUFFLE sur {machine_name}: FAIL")   


@timeit
def run_shuffle_on_cluster():
    """ Méthode permettant de lancer la fonction 'run_shuffle' sur tout le cluster de manière parallélisée """
    assert os.path.exists('machines.txt')
    pool = Pool(os.cpu_count())
    with open('machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map(run_shuffle, names)
        # Attente de la fin d'exécution
        pool.close()
        pool.join()
    print("SHUFFLE FINISHED")


# Rajouter des timeout
def run_reduce(machine_name: str) -> None:
    """ Méthode permettant de lancer la phase de reduce sur une machine """
    try:
        subprocess.run(f"ssh {LOGIN}@{machine_name.rstrip()} 'python3 /tmp/{LOGIN}/slave.py 2 /tmp/{LOGIN}'", shell=True, check=True)
        #print(f"Exécution du REDUCE sur {machine_name}: OK")
    except subprocess.CalledProcessError:
        print(f"Exécution du REDUCE sur {machine_name}: FAIL")   


@timeit
def run_reduce_on_cluster():
    """ Méthode permettant de lancer la fonction 'run_reduce' sur tout le cluster de manière parallélisée """
    assert os.path.exists('machines.txt')
    pool = Pool(os.cpu_count())
    with open('machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map(run_reduce, names)
        # Attente de la fin d'exécution
        pool.close()
        pool.join()
    print("REDUCE FINISHED")


@timeit
def split_data(path: Path, n: int) -> None:
    """ Méthode permettant de créer des splits à partir d'une données d'entrée"""
    # Comptage du nombre de lignes du fichier
    r = subprocess.run(f"wc -l {path}".split(), capture_output=True)
    n_lines = int(r.stdout.decode().split()[0])

    # Exécution du script de split
    Path().cwd().joinpath('split').mkdir(exist_ok=True)
    split_path = Path().cwd().joinpath('split')
    subprocess.run(f"split -d -e -l {n_lines // n + n_lines % n} {path} S".split(), cwd=split_path)


def display_result() -> None:
    """ Méthode permettant de visualiser les résulats du mapReduce sur les machines distantes """
    with open('machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
    print("============== Résultats ==============")
    for machine_name in names:
        r = subprocess.run(f"ssh {LOGIN}@{machine_name} cat /tmp/{LOGIN}/reduces/* | head -n 10".split(), capture_output=True)
        print(r.stdout.decode())


@timeit
def deploy() -> None:
    subprocess.run('python3 deploy.py'.split())



@timeit
def main():
    # On split les données en n_machine
    os.chdir(f"/tmp/{LOGIN}/")
    r = subprocess.run(f"wc -l /tmp/{LOGIN}/machines.txt".split(), capture_output=True)
    n_machine = int(r.stdout.decode().split()[0])
    split_data(Path.cwd().joinpath("data/input.txt"), n_machine)
    deploy()
    run_map_on_cluster()
    run_shuffle_on_cluster()
    run_reduce_on_cluster()


if __name__ == "__main__":
    main()
    display_result()
