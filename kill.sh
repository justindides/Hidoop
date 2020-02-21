# Définition des constantes
PATH_REPO='/home/cgarosai/git/HidoopMaster/hidoop/bin'
SUFFIX='.enseeiht.fr'
CLIENT='luke'${SUFFIX}
DAEMON1='malicia'${SUFFIX}
DAEMON2='yoda'${SUFFIX}
DAEMON3='palpatine'${SUFFIX}
DAEMON4='torvalds'${SUFFIX}
NAMENODE='liskov'${SUFFIX}

ssh cgarosai@${DAEMON1} "pkill java"
echo  "Daemon1 killed"
ssh cgarosai@${DAEMON2} "pkill java"
echo  "Daemon2 killed"
ssh cgarosai@${DAEMON3} "pkill java"
echo  "Daemon3 killed"
ssh cgarosai@${DAEMON4} "pkill java"
echo  "Daemon4 killed"
ssh cgarosai@${NAMENODE} "pkill java"
echo  "Namenode killed"

