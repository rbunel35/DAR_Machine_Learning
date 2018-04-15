Projet DAR de Karim Berkane, Richard Bunel, Patrick Chen

Le dossier upmc est un projet IntelliJ, qu'il est possible d'importer dans l'IDE et de builder.
Pour cela, il faut posséder le dataset ATP.csv, dont l'archive est contenue dans ce dépôt.
Il faudra également probablement modifier la variable "datafolder", ligne 36 dans App.scala, qui localise ATP.csv.

Notre build upmc.jar est cependant déjà présent dans upmc/out/artifacts/upmc_jar.

Pour exécuter le projet depuis la racine du dépôt Git, il faut faire la commande suivante:

spark-submit --class fr.upmc_insta.stl.dar.SparkTest --master local upmc/out/artifacts/upmc_jar/upmc.jar

Les résultats exportés seront placés dans des dossiers de A à G qui contiendront les CSV de nos 7 modèles.

Ici aussi, nos résultats sont déjà inclus dans le dépôt.

Enfin, les graphiques présents dans le rapport sont également présents.
