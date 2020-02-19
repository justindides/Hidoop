# Définition des constantes
PATH_REPO='/home/cgarosai/git/HidoopMaster/hidoop/bin'
SUFFIX='.enseeiht.fr'
CLIENT='luke'${SUFFIX}
DAEMON1='malicia'${SUFFIX}
DAEMON2='yoda'${SUFFIX}
DAEMON3='palpatine'${SUFFIX}
DAEMON4='torvalds'${SUFFIX}

ssh cgarosai@${DAEMON1} "pkill java"&
ssh cgarosai@${DAEMON2} "pkill java"&
ssh cgarosai@${DAEMON3} "pkill java"&
ssh cgarosai@${DAEMON4} "pkill java"&
