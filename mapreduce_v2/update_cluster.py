from subprocess import TimeoutExpired
import pandas as pd
import sys
import subprocess
import os
from multiprocessing import Pool
from pathlib import Path

LOGIN='fpont'


def update_machine_list(n: int) -> None:
    """
        Fonction qui va lire les machines dispos sur le site:
        https://supervision.enst.fr/tp/ et va choisir n machines 
        parmis toutes celles allumées et les répertorier dans le fichier
        'machines.txt' (on choisira en priorité les machines non utilisées)
    """
    try:
        data = pd.read_html("https://supervision.enst.fr/tp/")[0]
    except:
        print("Problème de lecture du site de référencement. Veuillez activer votre VPN ou réessayer plus tard")
        return

    # Suppression des machines éteintes et shuffle des machines
    data.drop(data[data.Statut == 'Indisponible'].index, inplace=True)
    data = data.sample(frac=1).reset_index(drop=True)
    # Ecrase le fichier précédent
    compt = 0
    with open("machines.txt", "w") as f:
        for machine in data["Hôtes"].values:
            if compt < n:
                try:
                    test_connection(machine_name=machine)
                except:
                    pass
                else:
                    f.write(machine + "\n")
                    compt += 1
            else:
                try:
                    test_connection(machine_name=machine)
                except:
                    pass
                else:
                    # On utilise cette nouvelle machine pour déployer notre master
                    print(f"Machine maître : {machine}")
                    with open('master.txt', 'w') as m:
                        m.write(machine)
                    break

    print(f"Le nouveau cluster contient {compt} machines.")


def test_connection(machine_name: str) -> None:
    """ Fonction permettant de tester la connexion à une machine """

    try:
        subprocess.run(f"ssh {LOGIN}@{machine_name} hostname".split(), timeout=20, check=True, capture_output=False)
    except subprocess.CalledProcessError:
        print(f"Statut {machine_name}: FAIL")
    except TimeoutExpired:
        print(f"Timeout expiré {machine_name}: FAIL")
    else:
        print(f"Statut {machine_name}: OK")



def test_connection_on_cluster(timeout: int=None) -> None:
    """ 
        Fonction permettant de tester la connexion aux machines du cluster de 
        manière parallélisée.
    """
    assert os.path.exists('machines.txt')
    pool = Pool(os.cpu_count())
    with open('machines.txt', 'r') as f:
        names = [name.rstrip() for name in f.readlines()]
        pool.map(test_connection, names)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Utilisation: python3 update_cluster.py n_machines")
    else:
        update_machine_list(int(sys.argv[1]))
