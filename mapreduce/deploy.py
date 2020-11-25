from multiprocessing import Pool
import os
from subprocess import TimeoutExpired 
import subprocess
from itertools import cycle
from config import LOGIN, Color
from pathlib import Path

def upload_slave(machine_name: str) -> None:
    """ 
        Fonction permettant d'uploader le slave et le fichier machines.txt
        sur une machine via rsync (plus rapide que scp) 
    """
    try:
        # La fonction run est synchrone c'est à dire que la copie du fichier va
        # attendre que la création du dossier soit terminée
        subprocess.run(f"rsync -pz slave.py config.py machines.txt used_machines.txt {LOGIN}@{machine_name.rstrip()}:/tmp/{LOGIN}/".split(), check=True)
    except subprocess.CalledProcessError:
        print(f"Déploiement du slave sur {machine_name}: {Color.RED}FAIL{Color.END}")


def upload_slave_on_cluster():
    """ Appelle la fonction "upload_slave" de manière parallèle sur tout le cluster """
    pool = Pool(os.cpu_count())
    with open('used_machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map(upload_slave, names)


def upload_data(machine_name: str, data_path: Path) -> None:
    """ 
        Fonction permettant d'uploader des données sur une machine via 
        rsync (plus rapide que scp) 
    """
    try:
        # Option -z pour compresser les données
        # La fonction run est synchrone c'est à dire que la copie du fichier va
        # attendre que la création du dossier soit terminée
        subprocess.run(f"ssh {LOGIN}@{machine_name} mkdir -p  /tmp/{LOGIN}/split".split(), check=True)
        subprocess.run(f"rsync -za {data_path} {LOGIN}@{machine_name}:/tmp/{LOGIN}/split/".split(), check=True)
    except subprocess.CalledProcessError:
        print(f"Déploiement de {data_path.split('/')[-1]} sur {machine_name}: {Color.RED}FAIL{Color.END}")


def upload_data_on_cluster(data_folder_path: Path):
    """ Appelle la fonction "upload_data" de manière parallèle sur tout le cluster """
    files = [data_folder_path.joinpath(name) for name in os.listdir(data_folder_path)]
    with open('machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
    # Si le nombre de split est plus petit que le nombre de machine on n'utilise pas toutes les machines
    names = names[:len(files)] if len(files) < len(names) else names
    # On note les machines utilisées
    with open("used_machines.txt", "w") as f:
        for machine in names:
            f.write(machine + "\n")
    with Pool(os.cpu_count()) as pool:
        # Si il y a plus de fichiers que de machines on cycle sur les machines
        pool.starmap(upload_data, list(zip(cycle(names), files)))
        pool.close()
        pool.join()


if __name__ == "__main__":
    os.chdir(f"/tmp/{LOGIN}/")
    upload_data_on_cluster(Path().cwd().joinpath('split'))
    upload_slave_on_cluster()
