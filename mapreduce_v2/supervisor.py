import sys
from pathlib import Path
import subprocess

LOGIN = 'fpont'


def load_data(path: Path) -> None:
    """ Fonction permettant de charger les données à traiter sur le master"""
    with open('master.txt', 'r') as f:
        master = f.readlines()[0]
    try:
        subprocess.run(f"ssh {LOGIN}@{master} mkdir -p /tmp/{LOGIN}/data".split())
    except:
        pass
    try:
        subprocess.run(f"rsync -zaP {path} {LOGIN}@{master}:/tmp/{LOGIN}/data/input.txt".split())
    except:
        print("Error")


def run() -> None:
    """ Fonction permettant de lancer le job map reduce """
    update()
    with open('master.txt', 'r') as f:
        master = f.readlines()[0]
    try:
        print("Lancement du job MapReduce")
        subprocess.run(f"ssh {LOGIN}@{master} python3 /tmp/{LOGIN}/master.py".split())
    except:
        print("Erreur d'exécution du MapReduce")


def update() -> None:
    """ Fonction permettant de mettre à jour les fichiers slaves et master sur le cluster """
    with open('master.txt', 'r') as f:
        master = f.readlines()[0]
    path = Path().cwd()
    try:   
        subprocess.run(f"rsync -za {path}/ {LOGIN}@{master}:/tmp/{LOGIN}/".split())
    except:
         print("Erreur de mise à jour")   
    else:
        print("Mise à jour des fichiers")


def clean_cluster() -> None:
    """ Fonction permettant de nettoyer tout le cluster """
    # Nettoyage du cluster
    with open('master.txt', 'r') as f:
        master = f.readlines()[0]
    try:   
        subprocess.run(f"ssh {LOGIN}@{master} python3 /tmp/{LOGIN}/clean.py".split())
    except:
         print("Erreur de nettoyage") 



if __name__ == "__main__":
    if (sys.argv[1] == 'load'):
        if (sys.argv[2] == ''):
            print("Utilisation: supervisor.py load path_to_data")
        else:
            try:
                path = Path().cwd().joinpath(sys.argv[2])
            except:
                print("Chemin inconnu")
            else:
                load_data(path)
    elif (sys.argv[1] == 'run'):
        run()
    elif (sys.argv[1] == 'update'):
        update()
    elif (sys.argv[1] == 'clean'):
        clean_cluster()
    else:
        print("Utiliser 'run', 'update', 'load'")
