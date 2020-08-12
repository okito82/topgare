/* -----------------------------------------------------------------
Projections :
- simples ou avec calculs (arithmetiques, concatenation)
- avec fonctions chaines, arithmetiques
- avec fonctions d'aggrégation (min, max, avg, sum, count ...)
- avec clause "AS"
- distinct
- tris
----------------------------------------------------------------- */


/* Nom et mel de tous les clients */
SELECT nom, email
FROM client

/* Dates d'attribution des clients aux commerciaux */
SELECT date_attribution
FROM portefeuille;

/* Date d'attribution sans doublon */
SELECT DISTINCT date_attribution
FROM portefeuille;

/* Longueur du email des clients (fonction chaine) */
SELECT email, length(email)
FROM client;

/* Nom des clients suivi de => email */
SELECT concat_ws(' => ', nom, email) AS client
FROM client;

SELECT concat(nom, ' => ', email)
FROM client;

/* Nom du client suivi du commentaire entre parenthèses (utiliser IFNULL) */
SELECT concat(nom, ifnull(concat(' (', commentaire, ')'), ''))
from client

/* Dates d'attribution, sans doublon (utiliser DISTINCT)*/
SELECT DISTINCT date_attribution
FROM portefeuille

/* date d'attribution des clients aux commerciaux, et nb de jours et de mois depuis */
SELECT date_attribution, sysdate(),
datediff(sysdate(), date_attribution) AS nb_jours,
timestampdiff(MONTH, date_attribution, sysdate()) as nb_mois
FROM portefeuille;

/* Date d'attribution, sous la forme jj/mm/aaaa */
SELECT date_format(date_attribution, '%d/%m/%y') as date
FROM portefeuille;

/* Nombre de clients, et nombre de commentaires */
SELECT count(*) AS nb_clients, count(commentaire) AS nb_commentaires
FROM client;

/* Nombre de comptes, et de clients différents dans la table compte */
SELECT count(no_client) as nb_comptes, count(distinct no_client) as nb_clients
from compte;

/* En cours total de la banque (somme des soldes) */
SELECT SUM(solde) AS encours_total
FROM compte;

/* Solde minimal, moyen, maximal, et écart-type, arrondis à 2 décimales */
SELECT
  MIN(solde) AS minimum,
  FORMAT(AVG(solde), 2) AS moyenne,
  MAX(solde) AS maximum,
  FORMAT(STDDEV(solde), 2) AS ecart_type
FROM compte;

/* Nombre de clients par n° de commercial (group by) */
SELECT no_commercial, count(no_client) AS nb_clients
FROM portefeuille
GROUP BY no_commercial;

/* Portefeuille trié par n° commercial et n° client */



/* -----------------------------------------------------------------
Restrictions :
- conditions simples
- null ou not null
- between avec nombres, dates et chaines
- like
----------------------------------------------------------------- */

/* Solde, n° de compte et de client des comptes de solde au moins égal à 2000 € */
SELECT no_compte, no_client, solde
FROM compte
WHERE solde >= 2000;

/* Idem pour les comptes de solde entre 1500 et 2000 (bornes incluses) */
SELECT no_compte, no_client, solde
FROM compte
WHERE solde BETWEEN 1500 AND 2000;

/* Clients dont le nom commence par D, que ce soit en majuscule ou minuscule */
SELECT *
FROM client
WHERE lower(nom) LIKE 'd%';

/* Clients dont le nom contient D, majuscule ou mnuscule */
SELECT *
FROM client
WHERE lower(nom) LIKE '%d%';

/* Clients sans commentaire (IS NULL) */
SELECT *
FROM client
WHERE commentaire IS NULL

/* Comptes dont le n° de client est dans la liste 1, 3 (IN) */
SELECT *
FROM compte
WHERE no_client in (1, 3);

/* Clients de nom commencant par T ou H */
SELECT *
FROM client
WHERE substr(nom, 1, 1) IN ('T', 'H');


/* -----------------------------------------------------------------
Jointures :
- naturelles
- requetes imbriquees
----------------------------------------------------------------- */
/* Comptes avec n°, nom du client et solde */
SELECT no_compte, nom AS nom_client, solde
FROM compte INNER JOIN client
ON compte.no_client = client.no_client;

/* Nom et email des clients, avec le n° de leurs commercials et la date d'attribution */
SELECT c.nom, c.email, p.no_commercial, date_attribution
FROM client c INNER JOIN portefeuille p ON c.no_client = p.no_client
ORDER BY nom;

/* Noms des commerciaux et de leur clients associés, et date d'attribution */
SELECT c.nom AS nom_commercial, cl.nom AS nom_client, date_attribution
FROM commercial c INNER JOIN portefeuille p ON c.no_commercial = p.no_commercial
INNER JOIN client cl ON p.no_client = cl.no_client
ORDER BY nom_commercial, nom_client;


/* N° et date d'attribution des commerciaux s'occupant du client Dupont */
SELECT no_commercial, date_attribution
FROM portefeuille p INNER JOIN client c ON p.no_client = c.no_client
WHERE nom = 'Dupont';

/* Idem avec requête imbriquée à 1 valeur */
SELECT no_commercial, date_attribution
FROM portefeuille
WHERE no_client =
(
  SELECT no_client
  FROM client
  WHERE nom = 'Dupont'
);

/* Infos des comptes du client de nom Dupont (requête imbriquée 1 valeur) */
SELECT *
FROM compte
WHERE no_client =
(
  SELECT no_client
  FROM client
  WHERE nom = 'Dupont'
);

/* Nom du commercial, date d'attribution et nom de leurs clients sans commentaire */
SELECT c.nom, date_attribution, cl.nom
FROM client cl INNER JOIN portefeuille p ON cl.no_client = p.no_client
INNER JOIN commercial c ON p.no_commercial = c.no_commercial
WHERE cl.commentaire IS NULL;

/* N°, nom et email des clients gérés par le commercial n° 1 */
SELECT c.no_client, email
FROM client c INNER JOIN portefeuille p ON c.no_client = p.no_client
WHERE no_commercial = 1;

/* Idem avec requête imbriquée à n valeurs */
SELECT * FROM client
WHERE no_client IN
(
  SELECT no_client
  FROM portefeuille
  WHERE no_commercial = 1
);

/* Infos des clients gérés par le commercial Lampion (requête imbriquée n valeurs) */
SELECT *
FROM client
WHERE no_client IN
(
  SELECT no_client
  FROM portefeuille
  WHERE no_commercial =
  (
    SELECT no_commercial
    FROM commercial
    WHERE nom='Lampion'
  )
);

/* Infos des commerciaux ayant géré le client 1 (requête imbriquée avec n valeurs) */
SELECT *
FROM commercial
WHERE no_commercial IN
(
  SELECT no_commercial
  FROM portefeuille
  WHERE no_client = 1
);

/* Comptes ayant un solde au moins égal au solde moyen (imbriquée 1 valeur) */
SELECT *
FROM compte
WHERE solde >=
(
  SELECT AVG(solde)
  FROM compte
);



/* Solde et n° de commercial des comptes du client Dupont (imbriquée avec 1 valeur) */
SELECT no_compte, solde, no_commercial
FROM
  compte c INNER JOIN portefeuille p
  ON c.no_client = p.no_client
WHERE c.no_client =
(
	SELECT no_client
	FROM client
	WHERE nom='Dupont'
);

/* N° de commercial et date d'attribution des commerciaux ayant géré Dupont (imbriquée 1 valeur) */
SELECT no_commercial, date_attribution
FROM portefeuille
WHERE no_client =
(
  SELECT no_client
  FROM client
  WHERE nom = 'Dupont'
);

/* N° des clients ayant même commercial et date d'attribution que Dupont (imbriquée avec plusieurs colonnes) */
SELECT DISTINCT no_client
FROM portefeuille
WHERE (no_commercial, date_attribution) IN
(
  SELECT no_commercial, date_attribution
  FROM portefeuille
  WHERE no_client =
  (
    SELECT no_client
    FROM client
    WHERE nom = 'Dupont'
  )
);

/* client avec leur compte de plus grand solde (imbriquée avec plusieurs colonnes) */
SELECT cl.no_client, nom, email, solde
FROM
  client cl INNER JOIN compte c ON cl.no_client=c.no_client
WHERE (no_client, solde) IN
(
  SELECT cl.no_client, MAX(solde)
  FROM client cl INNER JOIN compte c ON cl.no_client=c.no_client
  GROUP BY cl.no_client
);

/* Clients avec leur compte de plus grand solde (imbriquée avec plusieurs colonnes) */
SELECT *
FROM
	client cl
		INNER JOIN
	compte c ON cl.no_client = c.no_client
WHERE (c.no_client, solde) IN
(
	SELECT no_client, MAX(solde)
	FROM compte
	GROUP BY no_client
)

/* Clients avec le n° de leur dernier commercial attribué
(imbriquée avec plusieurs colonnes) */
SELECT *
FROM
	client c
    INNER JOIN
  portefeuille p	ON c.no_client = p.no_client
WHERE (p.no_client, date_attribution) IN
(
	SELECT no_client, MAX(date_attribution)
	FROM portefeuille
	GROUP BY no_client
)

/* Infos des clients avec le n° et le nom de leur commercial attribué
actuellement (le dernier), et la date d'attribution */
SELECT
	c.no_client, c.nom AS nom_client, email, commentaire,
	com.no_commercial, com.nom AS nom_commercial, date_attribution
FROM
	client c
		INNER JOIN
	portefeuille p	ON c.no_client = p.no_client
		INNER JOIN
	commercial com ON p.no_commercial = com.no_commercial
WHERE (p.no_client, date_attribution) IN
(
	SELECT no_client, MAX(date_attribution)
	FROM portefeuille
	GROUP BY no_client
)


/* N° et nom des commerciaux, avec le n° de client qu'ils ont gérés */
SELECT co.no_commercial, co.nom, p.no_client
FROM
  portefeuille p
  INNER JOIN commercial co
  ON p.no_commercial = co.no_commercial

/* N° de client, nom et solde total (tous comptes confondus) des clients (GROUP BY) */
SELECT nom, SUM(solde) AS solde_total
FROM
  compte c
  INNER JOIN client cl
  ON c.no_client = cl.no_client
GROUP BY nom;

/* Minimum, moyen et maximum des soldes totaux par client (tous comptes confondus) (GROUP BY) */
SELECT
  MIN(solde_cumule) AS total_minimum,
  AVG(solde_cumule) AS total_moyen,
  MAX(solde_cumule) AS total_maximum
FROM (
  SELECT no_client, SUM(solde) AS solde_cumule
  FROM compte
  GROUP BY no_client
) t1;

/* N°, nom et solde total des clients ayant un solde total au moins égal
à la moyenne des soldes totaux (HAVING) */
SELECT nom, SUM(solde) AS solde_total
FROM
  compte c
  INNER JOIN client cl
  ON c.no_client = cl.no_client
GROUP BY nom
HAVING solde_total >=
(
	SELECT AVG(solde_cumule)
	FROM (
		SELECT SUM(solde) AS solde_cumule
		FROM compte
		GROUP BY no_client
	) t1
);

/* N° et nom des commerciaux, avec le nombre de clients qu'ils ont gérés (OUTER JOIN) */
SELECT c.no_commercial, nom, COUNT(no_client) AS nb_clients
FROM
	commercial c
	LEFT OUTER JOIN portefeuille p
	ON c.no_commercial = p.no_commercial
GROUP BY c.no_commercial, nom;

/* N° client, nom, nb de comptes et solde total (OUTER JOIN) */
SELECT cl.no_client, nom, COUNT(no_compte) AS nb_comptes, IFNULL(SUM(solde), 0) AS solde_total
FROM
	client cl
	LEFT OUTER JOIN compte c
	ON cl.no_client = c.no_client
GROUP BY cl.no_client, nom;

/* Commerciaux sans client (avec NOT EXISTS) */
SELECT *
FROM commercial c
WHERE NOT EXISTS
(
	SELECT no_commercial
	FROM portefeuille p
	WHERE p.no_commercial = c.no_commercial
);

/* Commerciaux ayant géré tous les clients attribués (avec aggrégation) */
SELECT c.no_commercial, nom
FROM commercial c INNER JOIN portefeuille p ON c.no_commercial=p.no_commercial
GROUP BY c.no_commercial, nom
HAVING COUNT(no_client) =
(
  SELECT COUNT(DISTINCT no_client)
  FROM portefeuille
 );

/* Commerciaux ayant géré tous les clients attribués (avec NOT EXISTS) =
 commerciaux pour lesquels il n'existe aucun client pour lesquel
 il n'existe aucune association (portefeuille) avec ce commercial
 */
SELECT no_commercial, nom
FROM commercial c
WHERE NOT EXISTS
(
	SELECT no_client
	FROM portefeuille p
	WHERE NOT EXISTS
	(
		SELECT no_commercial
		FROM portefeuille p1
		WHERE
			p1.no_client = p.no_client
			AND p1.no_commercial = c.no_commercial
	)
);