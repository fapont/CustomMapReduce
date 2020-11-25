from os import write
import sys
from pathlib import Path
import socket
from collections import defaultdict
import subprocess
import hashlib
from multiprocessing import Pool
import os

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


def create_file_and_upload(filepath: Path, dest_machine: str, words_freq: dict) -> None:
    """
        Fonction servant à créer un fichier contenant les mots ainsi que leur fréquence 
        (inclus dans le dictionnare words_freq), et à déployer ce fichier sur la bonne machine
        
    """
    # Création du fichier
    with open(filepath, 'a', encoding='utf-8') as f:
        for word, freq in words_freq.items():
            f.write(f'{word} {freq} \n')
    # Envoi du fichier
    try:
        subprocess.run(f"ssh {LOGIN}@{dest_machine} mkdir -p  /tmp/{LOGIN}/shufflesreceived".split(), check=True)
        subprocess.run(f"rsync -za {filepath} {LOGIN}@{dest_machine}:/tmp/{LOGIN}/shufflesreceived/".split(), check=True)
    except subprocess.CalledProcessError:
        print(f"Shuffle {filepath.name} exporté sur {dest_machine}: FAIL")


def shuffle(folder_path: Path):
    """ 
        Fonction qui calcule le hash de tous les résultats de la phase de map et assigne
        chaque résultat à une machine en prenant le modulo de ce hash par le nombre de
        machines.
    """
    # Folder path est le chemin vers le fichier slave.py où se trouve les dossiers
    data_path = folder_path.joinpath('maps')
    result_path = folder_path.joinpath('shuffles')

    # Lecture de la liste des machines
    with open(folder_path.joinpath('machines.txt'), 'r', encoding='utf-8') as machines:
        machine_list = [val.rstrip() for val in machines.readlines()]
    n_machines = len(machine_list)

    # Définition de la fonction de hashage
    hash_string = lambda s: abs(int(hashlib.sha256(s.encode('utf-8')).hexdigest(), 16) % 10**8)

    # On stocke dans un dictionnaire les dictionnaires de mots qui doivent être exportés sur chaque machine
    # De plus, l'utilisation des defaultdict nous permet de compter de manière optimisée les instances
    node_repartions = defaultdict(lambda: defaultdict(int))
    hostname = socket.gethostname()
    for file in data_path.iterdir():
        with open(file, 'r') as f:
            for line in f.readlines():
                word = line.split()[0]
                # On calcule le numéro de la machine sur laquelle doit être envoyé le mot grâce au hash % n_machines
                node_repartions[hash_string(word) % n_machines][word] += 1

    paths = []
    machines = []
    dictionnaries = []
    # On itère sur les clés du dictionnaire (qui est l'id de la machine sur laquelle écrire)
    for id_machine, dic in node_repartions.items():
        machines.append(machine_list[id_machine])
        paths.append(result_path.joinpath(f'file{id_machine}-{hostname}.txt') )
        dictionnaries.append(dic)
    

    # Parallélisation de la création des fichiers et du transfert
    with Pool(os.cpu_count()) as pool:
        pool.starmap(create_file_and_upload, list(zip(paths, machines, dictionnaries)))
        pool.close()
        pool.join()


def reducing(folder_path: Path):
    """ Fonction de reduce qui va comptabiliser toutes les occurences des mots qui ont été attribués à la machine """
    data_path = folder_path.joinpath('shufflesreceived')
    result_path = folder_path.joinpath('reduces')

    # On itère sur tous les fichiers du dossier 'shufflesreceived' et on comptabilise les occurences
    count = defaultdict(int)
    for file in data_path.iterdir():
        with open(file, 'r') as f:
            for line in f.readlines():
                word, freq = line.split()
                count[word] += int(freq)

    # On écrit les résultats dans un nouveau fichier
    with open(result_path.joinpath("result.txt"), "w", encoding='utf-8') as out:
        for word, freq in count.items():
            out.write(f'{word} {freq} \n')
    

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