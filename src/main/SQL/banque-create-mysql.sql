DELIMITER §
DROP SCHEMA IF EXISTS banque §
CREATE SCHEMA IF NOT EXISTS banque DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci §
USE banque §

CREATE TABLE IF NOT EXISTS client (
  no_client INT(11) NOT NULL AUTO_INCREMENT,
  nom VARCHAR(45) NOT NULL,
  email VARCHAR(45) NOT NULL,
  commentaire VARCHAR(255),
  PRIMARY KEY (no_client))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8 §


CREATE TABLE IF NOT EXISTS commercial (
  no_commercial INT(11) NOT NULL AUTO_INCREMENT,
  nom VARCHAR(45) NOT NULL,
  PRIMARY KEY (no_commercial))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8 §


CREATE TABLE IF NOT EXISTS compte (
  no_compte INT(11) NOT NULL AUTO_INCREMENT,
  solde DECIMAL(10,2) NOT NULL,
  no_client INT(11) NOT NULL,
  PRIMARY KEY (no_compte),
  CONSTRAINT fk_compte_client
    FOREIGN KEY (no_client)
    REFERENCES client (no_client))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8 §

CREATE TABLE IF NOT EXISTS portefeuille (
  no_commercial INT(11) NOT NULL,
  no_client INT(11) NOT NULL,
  date_attribution DATE NOT NULL,
  PRIMARY KEY (no_commercial, no_client),
  CONSTRAINT portefeuille_commercial
    FOREIGN KEY (no_commercial)
    REFERENCES commercial (no_commercial),
  CONSTRAINT portefeuille_client
    FOREIGN KEY (no_client)
    REFERENCES client (no_client))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8 §




DROP PROCEDURE IF EXISTS banque_reset §

CREATE PROCEDURE banque_reset()
BEGIN
  -- Désactiver contraintes de clé étrangère
  SET FOREIGN_KEY_CHECKS = 0;
  -- Vider les tables en repositionnant à 1 leur auto-incrément
  TRUNCATE TABLE client;
  TRUNCATE TABLE commercial;
  TRUNCATE TABLE compte;
  TRUNCATE TABLE portefeuille;
  -- Réasactiver contraintes de clé étrangère
  SET FOREIGN_KEY_CHECKS = 1;

  BEGIN
    -- Recuperation en cas d'exception
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      -- Annuler la transaction
      ROLLBACK;
      SELECT 'Insertions annullées. La base est vide à présent;';
    END;
    START TRANSACTION;
    INSERT INTO client (no_client, nom, email, commentaire) VALUES
    (1, 'Dupont', 'dupont@interpol.com', 'Client distrait. Je dirai même plus ...'),
    (2, 'Tintin', 'tintin@herge.be', NULL),
    (3, 'Haddock', 'haddock@moulinsart.fr', 'Grand amateur de Loch Lhomond'),
    (4, 'Castafiore', 'bianca@scala.it', 'A flatter. Ne surtout pas faire chanter');

    INSERT INTO commercial (no_commercial, nom) VALUES
    (1, 'Lampion'),
    (2, 'de Oliveira'),
    (3, 'Rastapopoulos');

    INSERT INTO compte (no_compte, solde, no_client) VALUES
    (1, '1000.00', 1),
    (2, '1500.00', 1),
    (3, '2000.00', 2),
    (4, '2500.00', 3);

    INSERT INTO portefeuille (no_commercial, no_client, date_attribution) VALUES
    (1, 1, '2005-12-23'),
    (1, 2, '2010-04-21'),
    (1, 3, '2015-04-12'),
    (2, 1, '2015-04-12');
    COMMIT;
  END;
END §

CALL banque_reset();