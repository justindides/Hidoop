# Hidoop

PROJET INTERGICIEL SEMESTRE 1 ASR N7

ETAT FINAL :

HDFS ET NAMENODE : 
Hdfs fonctionne indépendament d'Hidoop comme demandé, les commandes write, read et delete sont les mêmes que dans le sujet.
Cependant, afin de stocker les informations concernant la répartitions des blocs sur les serveurs on a créer un serveur (RMI),
appelé "NameNode" (src/ordo/NameNode). Le namenode possède un un objet projet (src/config/projet), qui contient toutes les informations
sur la configurations des serveurs, port etc... ainsi que la répartition des blocs sur ces serveurs. 
Lorsque Hdfs réalise une action, il va mettre à jour ce projet dans le Namenode. Pour d'Hdfs fonctionne, il faut que le Namenode soit
lancé au préalable. Lorsqu'il écrit, hdfs doit rajouter des choses dans la répartition des blocs, et lorsqu'il lit ou delete il doit
acceder à cette même donnée, c'est pour cela que le même Namenode doit rester lancé pendant toute la duréee que l'on 
utilisation d'Hdfs. C'est assez mauvais et pourrait être amélioré en faisant un Namenode se mettant à jour tout seul et dynamiquement 
lorsqu'on le démare. Deux implantations possible serait :
1- Du polling sur chaque serveur Hdfs pour récupérer les blocs contenus par chacun.
2- Un fichier de récupératiob ou serait décrit la répartition des blocs. 

Attention à donc bien lancer un namenode, et ne pas le fermer durant toute l'utilisation de l'application.

HIDOOP (MyMapReduce) : 
Hidoop s'éxecute grâce à la clase MyMapReduce, qui prend en argument un string qui est le nom du fichier que l'on veut traiter. Il faut 
donc que ce fichier ait été auparavant répartie par Hdfs et que le Namenode soit lancé pour récupérer les infos. MyMapReduce va ensuite
lancer le map sur tous les fragments grâce aux daemons présents sur les machines contenant les fragments. Il faut bien faire la disctinction
entre les daemons hidoop et les serveurs Hdfs qui sont des programmes différents, mais qui sont liés par la machnie sur laquelle ils 
s'exécutent. 
MyMapReduce va ensuite lancé un Hdfs read pour réunir tous les fragments traités sur la machine du MyMapReduce. Puis un réalise un reduce
sur ce dernier fichier.

Chronologie et nom des fichiers : Fichier.txt -> Hdfs write (Fichier.txt) = Fichier-bloc-i.txt ->
-> Map(De chaque Fichier-bloc-i.txt) = Fichier-res-i.txt -> Hdfs read (Tous les Fichier-res-i.txt) = Fichier-concatenated.txt ->
-> Reduce(Fichier-concatenated.txt) = Fichier-rest.txt


CONFIGURATION (= Fichier des configs) : 
Quasiment toute la configuration du projet est contenu dans des fichiers de configuration, qui sont récupérer et traiter par la 
classe Projet (src/config/projet). Ces derniers sont les suivants :
data/hdfsClient/namenode.url et data/job/namenode.url -> URL du Namenode (serveur RMI)
data/hdfsClient/portNodes.conf -> Liste des ports sockets entre HdfsClient et HdfsServer
data/hdfsClient/servHdfs.listofurl -> Liste des serveurs Hdfs (nom d'Hote) à utiliser en plus des ports pour la création des sockets
data/hdfsClient/daemons.listofurl -> Les des daemons Hidoop (url des machines) qui sont respectivement sur les machines correspondant
                                      à leur serveur hdfs. 
data/hdfsClient/structure.conf -> Liste des fichiers que l'on veut traier. Obsolète, on pensait au départ qu'on devez exécuter notre 
                                   application sur plusieurs fichiers, on stoquait donc leur nom dans ce fichier.
