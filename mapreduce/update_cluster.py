from subprocess import CalledProcessError, TimeoutExpired
import pandas as pd
import sys
import subprocess
import os
from multiprocessing import Pool
from config import LOGIN, Color


def update_machine_list(n: int) -> None:
    """
        Fonction qui va lire les machines dispos sur le site:
        https://supervision.enst.fr/tp/ et va choisir n machines 
        parmis toutes celles allumées et les répertorier dans le fichier
        'machines.txt' (on choisira en priorité les machines non utilisées)
    """
    try:
        data = pd.read_html("https://supervision.enst.fr/tp/")[0]
    except Exception:
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
                except Exception:
                    pass
                else:
                    f.write(machine + "\n")
                    compt += 1
            else:
                try:
                    test_connection(machine_name=machine)
                except Exception:
                    pass
                else:
                    # On utilise cette nouvelle machine comme master
                    print(f"Machine maître : {machine}")
                    with open('master.txt', 'w') as m:
                        m.write(machine)
                    break
    print(f"\nLe nouveau cluster contient {compt} machines")


def test_connection(machine_name: str) -> None:
    """ Fonction permettant de tester la connexion à une machine """
    try:
        a = subprocess.run(f"ssh {LOGIN}@{machine_name} hostname".split(), timeout=10, \
            check=True, capture_output=True)
    except CalledProcessError:
        print(f"Statut {machine_name}: {Color.RED}FAIL{Color.END}")
        raise CalledProcessError
    except TimeoutExpired:
        print(f"Timeout expiré {machine_name}: {Color.RED}FAIL{Color.END}")
        raise TimeoutError
    else:
        print(f"Statut {machine_name}: {Color.GREEN}OK{Color.END}")



def test_connection_on_cluster() -> None:
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
    if (sys.argv[1] == 'test'):
        test_connection_on_cluster()
    elif len(sys.argv) != 2:
        print("Utilisation: python3 update_cluster.py n_machines")
    else:
        update_machine_list(int(sys.argv[1]))
