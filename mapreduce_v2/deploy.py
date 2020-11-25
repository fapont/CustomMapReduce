from multiprocessing import Pool
import os
from subprocess import TimeoutExpired 
import subprocess
from itertools import cycle

LOGIN='fpont'


def upload_slave(machine_name: str) -> None:
    """ 
        Fonction permettant d'uploader le slave et le fichier machines.txt
        sur une machine via rsync (plus rapide que scp) 
    """
    try:
        # La fonction run est synchrone c'est à dire que la copie du fichier va
        # attendre que la création du dossier soit terminée
        subprocess.run(f"rsync -p slave.py machines.txt {LOGIN}@{machine_name.rstrip()}:/tmp/{LOGIN}/".split(), check=True)
        # print(f"Déploiement du slave sur {machine_name}: OK")
    except subprocess.CalledProcessError:
        print(f"Déploiement du slave sur {machine_name}: FAIL")


def upload_slave_on_cluster():
    """ Appelle la fonction "upload_slave" de manière parallèle sur tout le cluster """
    assert os.path.exists('machines.txt')
    pool = Pool(os.cpu_count())
    with open('machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map(upload_slave, names)


def upload_data(machine_name: str, data_path: str) -> None:
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
        # print(f"Déploiement de {data_path.split('/')[-1]} sur {machine_name}: OK")
    except subprocess.CalledProcessError:
        print(f"Déploiement de {data_path.split('/')[-1]} sur {machine_name}: FAIL")


def upload_data_on_cluster(data_folder_path: str):
    """ Appelle la fonction "upload_data" de manière parallèle sur tout le cluster """
    assert os.path.exists('machines.txt')
    assert os.path.exists(data_folder_path)
    # Création du chemin absolu vers les fichiers
    if not data_folder_path.endswith('/'):
        data_folder_path += '/'
    data_folder_path = os.getcwd() + '/' + data_folder_path

    with Pool(os.cpu_count()) as pool:
        with open('machines.txt', 'r') as f:
            # On déploie 1 split par machine au minimum. S'il y a plus de split
            # que de machines, alors on déploie les splits sur certaines machines
            # en faisant attention d'optimiser la subdivision
            names = [name.rstrip() for name in f.readlines()]
            files = [data_folder_path + name for name in os.listdir(data_folder_path)]
            pool.starmap(upload_data, list(zip(cycle(names), files)))
            pool.close()
            pool.join()


if __name__ == "__main__":
    upload_slave_on_cluster()
    upload_data_on_cluster('split/')
