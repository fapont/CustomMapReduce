# Contenu de l'archive
Cette archive contient une implémentation incrémentale d'un **wordcount** distribué basé sur le paradigme de programmation **MapReduce**.

Cette implémentation incrémentale se base sur la proposition de Rémi Sharrock (enseignant à Télécom Paris) accessible sur son site:
https://remisharrock.fr/courses/simple-hadoop-mapreduce-from-scratch/

On retrouve donc 3 implémentations de mapreduce :
- **mapreduce_v1**: implémentation simple et peu user friendly permettant de faire des premiers tests sur des petits ficiers
- **mapreduce_v2**: implémentation optimisée mais non robuste permettant de gérer de gros fichiers et de battre le programme séquentiel 
- **mapreduce**: implémentation finale optimisée et "robuste" permettant une utilisation simplifiée

# Prérequis
- Python3.7+
- Accès au VPN de Télécom Paris

# Utilisation

Avant tout, veuillez vous assurer que vous êtes **connectés au VPN**.

Afin de récupérer une liste de *n machines* qui composeront notre cluster nous utilisons la commande suivante:
```
> python3 update_cluster.py n
```

Une fois le cluster établi, il nous suffit de charger la donnée voulue sur le Edge Node (ou master node dans notre cas):
```
> python3 supervisor.py load path_to_data
```

Puis, pour mettre à jouer tous les fichiers **slave** et **master** sur les machines et lancer le job mapreduce, il suffit de lancer la commande suivante:
```
> python3 supervisor.py run
```

Enfin, une fois le résultat obtenu, on peut nettoyer notre cluster et supprimer tous les fichiers présents sur les machines:
```
> python3 supervisor.py clean
```

**Note**: 
- Par défaut, la commande *run* affiche les 10 premiers résultats triés par ordre décroissant (paramètre à changer dans le fichier *master*)
- Les commandes suivantes sont utilisées dans la dernière version de mapreduce

# Informations

Pour plus d'informations, veuillez lire le rapport [rapport/rapport.pdf]().