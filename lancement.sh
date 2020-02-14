# Définition des constantes
PATH_REPO='/home/cgarosai/git/HidoopMaster/hidoop/src/'
SUFFIX='.enseeiht.fr'
CLIENT='luke'${SUFFIX}
DAEMON1='malicia'${SUFFIX}
DAEMON2='yoda'${SUFFIX}
DAEMON3='palpatine'${SUFFIX}
DAEMON4='torvalds'${SUFFIX}



# copie vers les machines de la clé
ssh-copy-id cgarosai@${SERVER}
ssh-copy-id cgarosai@${DAEMON1}
ssh-copy-id cgarosai@${DAEMON2}
ssh-copy-id cgarosai@${DAEMON3}
ssh-copy-id cgarosai@${DAEMON4}

# Lancement des daemons
ssh cgarosai@${DAEMON1} "cd ${PATH_REPO} && java ordo.DaemonImpl 1 &"
ssh cgarosai@${DAEMON2} "cd ${PATH_REPO} && java ordo.DaemonImpl 2 &"
ssh cgarosai@${DAEMON3} "cd ${PATH_REPO} && java ordo.DaemonImpl 3 &"
ssh cgarosai@${DAEMON4} "cd ${PATH_REPO} && java ordo.DaemonImpl 4 &"

# Lancement du namenode par le daemon1
ssh cgarosai@${DAEMON1} "cd ${PATH_REPO} && java ordo.NameNode"

# Lancement des servers
ssh cgarosai@${DAEMON1} "cd ${PATH_REPO} && java hdfs.HdfsServeur 8001 &"
ssh cgarosai@${DAEMON2} "cd ${PATH_REPO} && java hdfs.HdfsServeur 8001 &"
ssh cgarosai@${DAEMON3} "cd ${PATH_REPO} && java hdfs.HdfsServeur 8001 &"
ssh cgarosai@${DAEMON4} "cd ${PATH_REPO} && java hdfs.HdfsServeur 8001 &"


# Lancement du client
ssh cgarosai@${CLIENT} "cd ${PATH_REPO} && java hdfs.HdfsClient write line test.txt"
ssh cgarosai@${CLIENT} "cd ${PATH_REPO} && java application.MyMapReduce test.txt"
