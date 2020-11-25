import sys
from pathlib import Path
import socket
from collections import defaultdict
import subprocess
import hashlib

LOGIN = 'fpont'


def mapping(folder_path: Path):
    """ Fonction qui va effectuer la phase de map en reportant toutes les occurences de chaque mot suivi du chiffre 1 """
    # Folder path est le chemin vers le fichier slave.py où se trouve les dossiers
    data_path = folder_path.joinpath('split')
    result_path = folder_path.joinpath('maps')
    for file in data_path.iterdir():
        num = file.name[1] # fichiers de la forme Sx.txt
        with open(file, 'r') as f, open(result_path.joinpath(f'UM{num}.txt'), 'w') as out:
            for line in f.readlines():
                words = line.split()
                for word in words:
                    out.write(word + " 1\n")         


def shuffle(folder_path: Path):
    """ 
        Fonction qui calcule le hash de tous les résultats de la phase de map et assigne
        chaque résultat à une machine en prenant le modulo de ce hash par le nombre de
        machines.
    """
    assert folder_path.joinpath('machines.txt').exists()
    # Folder path est le chemin vers le fichier slave.py où se trouve les dossiers
    data_path = folder_path.joinpath('maps')
    result_path = folder_path.joinpath('shuffles')

    # On itère sur tous les fichiers du dossier maps/ et on calcule le hash. Afin d'optimiser
    # l'I/O on stocke tous les résultats dans un dictionnaire avant d'écrire dans les fichiers
    result = defaultdict(list)
    hostname = socket.gethostname()
    for file in data_path.iterdir():
        with open(file, 'r') as f:
            for line in f.readlines():
                hash_string = lambda s: int(hashlib.sha256(s.encode('utf-8')).hexdigest(), 16) % 10**8
                result[str(abs(hash_string(line.split()[0])))].append(line.rstrip())
    # Ecriture des résultats dans les fichiers
    for hashcode, content in result.items():
        with open(result_path.joinpath(f'{hashcode}-{hostname}.txt'), 'a', encoding='utf-8') as f:
            for line in content:
                f.write(line + "\n")

    # Lecture de la liste des machines
    with open(folder_path.joinpath('machines.txt'), 'r') as machines:
        machine_list = [val.rstrip() for val in machines.readlines()]

    # Transfert des fichiers sur les machines désignées à partir du calcul du hash
    for file in result_path.iterdir():
        # On récupère le nom de la machine sur laquelle doit être copié le fichier
        machine_name = machine_list[int(file.name.split('-')[0]) % len(machine_list)]
        # Si la machine sur laquelle on doit exporter le fichier est différentes de
        # celle qui exécute actuellement ce programme, on copie le fichier par SSH
        try:
            subprocess.run(f"ssh {LOGIN}@{machine_name} mkdir -p  /tmp/{LOGIN}/shufflesreceived".split(), check=True)
            subprocess.run(f"rsync -za {file} {LOGIN}@{machine_name}:/tmp/{LOGIN}/shufflesreceived/".split(), check=True)
            #print(f"Shuffle {file.name} exporté sur {machine_name}: OK")
        except subprocess.CalledProcessError:
            print(f"Shuffle {file.name} exporté sur {machine_name}: FAIL")



def reducing(folder_path: Path):
    """ Fonction de reduce qui va comptabiliser toutes les occurences des mots qui ont été attribués à la machine """
    data_path = folder_path.joinpath('shufflesreceived')
    result_path = folder_path.joinpath('reduces')

    # On itère sur tous les fichiers du dossier 'shufflesreceived', et on regroupe tous
    # les fichiers commençant par le même hash
    files = defaultdict(list)
    for file in data_path.iterdir():
        files[file.name.split('-')[0]].append(file) # On rajoute le nom de fichier complet associé à chaque hash
    
    for hashcode, files_list in files.items():
        with open(result_path.joinpath(f"{hashcode}.txt"), "w", encoding='utf-8') as f:
            nb = 0
            for file in files_list:
                with open(file, 'r') as inf:
                    nb += len(inf.readlines())
                    inf.seek(0)
                    name = inf.readline().split()[0]
            f.write(f"{name} {nb}")

    

if __name__ == "__main__":
    assert len(sys.argv) == 3, "Utilisation: python3 slave.py mode_de_fonctionnement dossier"
    if (sys.argv[1] == '0'):
        Path(sys.argv[2]).joinpath('maps').mkdir(exist_ok=True)
        mapping(Path(sys.argv[2]))
    elif (sys.argv[1] == '1'):
        Path(sys.argv[2]).joinpath('shuffles').mkdir(exist_ok=True)
        shuffle(Path(sys.argv[2]))
    elif (sys.argv[1] =='2'):
        Path(sys.argv[2]).joinpath('reduces').mkdir(exist_ok=True)
        reducing(Path(sys.argv[2]))
    else:
        print("Mauvais argument")