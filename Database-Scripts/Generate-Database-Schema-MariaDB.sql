-- ----------------------------------------------------------
-- EXOSKELETON TABLE STRUCTURE FOR MARIADB
-- for version 0.9.0 beta of exoskeleton
-- © 2019-2020 Rüdiger Voigt
-- APACHE-2 LICENSE
--
-- This file generates the table structure needed for the
-- exoskeleton framework in MariaDB.
--
-- For the current version and a documentation of the package,
-- please see:
-- https://github.com/RuedigerVoigt/exoskeleton
--
-- BEWARE: This should be run in an empty database as otherwise
-- this script might alter existing tables!
--
-- ----------------------------------------------------------

-- DO NOT FORGET TO CHANGE:
CREATE DATABASE `nameOfYourDatabase` DEFAULT CHARACTER SET utf8mb4;
USE `nameOfYourDatabase`;

-- ----------------------------------------------------------
-- QUEUE
--
-- Needed by the bot to know what to do next.
--
-- IMPORTANT: The queue must not use an integer with auto-increment
-- as an id.
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS queue (
    -- ID contains an UUID generated by Python instead of
    -- autoincrement, because:
    -- * After server restart the autoincrement counter is reset to
    --   MAX() which can lead to overwriting of files.
    -- * MariaDB generates different strings dependent on the OS.
    --   Python is RFC 4122 compliant and can be configured NOT to
    --   incorporate the machine's MAC address.
    -- * This spares a roundtrip to the DBMS as for compatibility
    --   reasons this cannot use INSERT .. RETURN and the script
    --   would have to query for the id using the URL.
    id CHAR(32) CHARACTER SET ASCII NOT NULL
    ,action TINYINT UNSIGNED
    -- MySQL does not no the alias BOOLEAN, so stick to TINYINT:
    ,prettifyHtml TINYINT UNSIGNED DEFAULT 0
    ,url TEXT NOT NULL
    ,urlHash CHAR(64) NOT NULL
    ,addedToQueue TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,causesError INT NULL
    ,numTries INT DEFAULT 0
    ,delayUntil TIMESTAMP NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`action`)
    ,UNIQUE(`urlHash`)
    ,INDEX(`addedToQueue`)
    ,INDEX(`delayUntil`)
) ENGINE=InnoDB;


-- ----------------------------------------------------------
-- JOBS
-- ----------------------------------------------------------

CREATE TABLE jobs (
    jobName VARCHAR(127) NOT NULL
    ,created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,finished TIMESTAMP NULL
    ,startUrl TEXT NOT NULL
    ,startUrlHash CHAR(64) NOT NULL
    ,currentUrl TEXT NULL
    ,PRIMARY KEY(`jobName`)
    ,INDEX(`created`)
    ,INDEX(`finished`)
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- ACTIONS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS actions (
    id TINYINT UNSIGNED NOT NULL
    ,description VARCHAR(256) NOT NULL
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;

INSERT INTO actions (id, description) VALUES
(1, 'download file to disk'),
(2, 'save page code into database'),
(3, 'save as PDF using headless Chrome');


ALTER TABLE `queue`
ADD CONSTRAINT `actions`
FOREIGN KEY (`action`)
REFERENCES `actions`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

-- ----------------------------------------------------------
-- ERRORS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS errorType (
    id INT NOT NULL
    ,short VARCHAR(31)
    ,description VARCHAR(255)
    ,permanent BOOLEAN
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;

INSERT INTO errorType (id, short, permanent, description) VALUES
(1, 'malformed url', 1, 'URL is malformed. Missing Schema (http:// or https://) ?'),
(2, 'transaction fails', 0, 'The database transaction failed. Rollback was initiated.'),
(402, '402', 1, 'Server replied: Payment Required'),
(403, '403', 1, 'Server replied: Forbidden'),
(404, '404', 1, 'Server replied: File not found'),
(405, '405', 1, 'Server replied: Method Not Allowed'),
(410, '410', 1, 'Server replied: Gone'),
(414, '414', 1, 'Server replied: URI too long'),
(429, '429', 0, 'Server replied: Too Many Requests'),
(451, '451', 1, 'Server replied: Unavailable For Legal Reasons'),
(500, '500', 0, 'Server replied: Internal Server Error'),
(503, '503', 0, 'Server replied: Service Unavailable');

ALTER TABLE `queue`
ADD CONSTRAINT `errors`
FOREIGN KEY (`causesError`)
REFERENCES `errorType`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;


-- ----------------------------------------------------------
-- FILE STORAGE TYPES
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS storageTypes (
    id INT UNSIGNED NOT NULL
    ,shortName VARCHAR(15)
    ,fullName VARCHAR(63)
    ,PRIMARY KEY(`id`)
    ) ENGINE=InnoDB;

INSERT INTO storageTypes (id, shortName, fullName) VALUES
(1, 'Database', 'Local Database'),
(2, 'Filesystem', 'Local Filesystem'),
(3, 'AWS', 'Amazon Web Services'),
(4, 'GCP', 'Google Cloud Platform'),
(5, 'Azure', 'Microsoft Azure'),
(6, 'Alibaba', 'Alibaba');


-- ----------------------------------------------------------
-- FILE MASTER
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS fileMaster (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,url TEXT NOT NULL
    ,urlHash CHAR(64) NOT NULL
    ,numVersions_t INT DEFAULT 0
    -- increment / decrement of version count via trigger in fileVersions
    -- Default has to be 0 not NULL as increment otherwise does not work
    ,PRIMARY KEY(`id`)
    ,UNIQUE(urlHash)
    ) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- FILE VERSIONS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS fileVersions (
    id CHAR(32) CHARACTER SET ASCII NOT NULL -- the UUID
    ,fileMasterID INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,storageTypeID INT UNSIGNED NOT NULL
    ,fileName VARCHAR(255) NULL
    ,mimeType VARCHAR(127) NULL
    ,pathOrBucket VARCHAR(2048) NULL
    ,versionTimestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,size INT UNSIGNED NULL
    ,hashMethod VARCHAR(6) NULL
    ,hashValue VARCHAR(512) NULL
    ,comment VARCHAR(256) NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`fileMasterID`)
    ,INDEX(`storageTypeID`)
    -- No index on fileName as too long for standard index
    ) ENGINE=InnoDB;


ALTER TABLE `fileVersions`
ADD CONSTRAINT `link from version to master`
FOREIGN KEY (`fileMasterID`)
REFERENCES `fileMaster`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

ALTER TABLE `fileVersions`
ADD CONSTRAINT `location-type`
FOREIGN KEY (`storageTypeID`)
REFERENCES `storageTypes`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

CREATE TRIGGER increment_version_count
AFTER INSERT ON fileVersions
FOR EACH ROW
UPDATE fileMaster
SET fileMaster.numVersions_t = (fileMaster.numVersions_t + 1)
WHERE fileMaster.id = NEW.fileMasterID;

DELIMITER //

CREATE TRIGGER decrement_version_count_or_delete_master
AFTER DELETE ON fileVersions
FOR EACH ROW
BEGIN

  UPDATE fileMaster
  SET fileMaster.numVersions_t = (fileMaster.numVersions_t - 1)
  WHERE fileMaster.id = OLD.fileMasterID;

  -- in case there is no version left: drop the file from the master table
  DElETE FROM fileMaster
  WHERE fileMaster.id = OLD.fileMasterID AND fileMaster.numVersions_t = 0;

END

//

DELIMITER ;


-- ----------------------------------------------------------
-- FILE CONTENT
--
-- Aistinct table instead of a filed in fileVersions in case
-- somebody uses "SELECT *". All version infos are
-- stored in fileVersions.
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS fileContent (
    versionID CHAR(32) CHARACTER SET ASCII NOT NULL
    ,pageContent MEDIUMTEXT NOT NULL
    ,PRIMARY KEY(`versionID`)
) ENGINE=InnoDB;

ALTER TABLE `fileContent`
ADD CONSTRAINT `link from fileContent to fileVersions`
FOREIGN KEY (`versionID`)
REFERENCES `fileVersions`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

CREATE TRIGGER removeContent
AFTER DELETE ON fileContent
FOR EACH ROW
DElETE FROM fileVersions
WHERE fileVersions.id = OLD.versionID;

-- ----------------------------------------------------------
-- STATISTICS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS statisticsHosts (
    fqdnHash CHAR(32) NOT NULL
    ,fqdn VARCHAR(255) NOT NULL
    ,firstSeen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
    ,lastSeen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP()
    ,successful INT UNSIGNED NOT NULL DEFAULT 0
    ,problems INT UNSIGNED NOT NULL DEFAULT 0
    ,PRIMARY KEY(`fqdnHash`)
    ,INDEX(`firstSeen`)
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- LABELS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS labels (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,shortName VARCHAR(63) NOT NULL
    ,description TEXT DEFAULT NULL
    ,PRIMARY KEY(`id`)
    ,UNIQUE(`shortName`)
    ) ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS labelToQueue (
    id INT NOT NULL AUTO_INCREMENT
    ,labelID INT UNSIGNED NOT NULL
    ,queueID CHAR(32) CHARACTER SET ASCII NOT NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`labelID`)
    ,INDEX(`queueID`)
    ) ENGINE=InnoDB;

ALTER TABLE `labelToQueue`
ADD CONSTRAINT `l2q labelID to id in labels`
FOREIGN KEY (`labelID`)
REFERENCES `labels`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

ALTER TABLE `labelToQueue`
ADD CONSTRAINT `l2q queueID to id in queue`
FOREIGN KEY (`queueID`)
REFERENCES `queue`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;



CREATE TABLE IF NOT EXISTS labelToMaster (
    id INT NOT NULL AUTO_INCREMENT
    ,labelID INT UNSIGNED NOT NULL
    ,masterID INT UNSIGNED NOT NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`labelID`)
    ,INDEX(`masterID`)
    ) ENGINE=InnoDB;

ALTER TABLE `labelToMaster`
ADD CONSTRAINT `l2m labelID to id in labels`
FOREIGN KEY (`labelID`)
REFERENCES `labels`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

ALTER TABLE `labelToMaster`
ADD CONSTRAINT `l2m masterID to id in fileMaster`
FOREIGN KEY (`masterID`)
REFERENCES `fileMaster`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;



CREATE TABLE IF NOT EXISTS labelToVersion (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,labelID INT UNSIGNED NOT NULL
    ,versionID CHAR(32) CHARACTER SET ASCII NOT NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`labelID`)
    ,INDEX(`versionID`)
    ) ENGINE=InnoDB;

ALTER TABLE `labelToVersion`
ADD CONSTRAINT `l2v labelID to id in labels`
FOREIGN KEY (`labelID`)
REFERENCES `labels`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

ALTER TABLE `labelToVersion`
ADD CONSTRAINT `l2v versionID to id in fileVersions`
FOREIGN KEY (`versionID`)
REFERENCES `fileVersions`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

-- ----------------------------------------------------------
-- VIEWS
--
-- For easier access.
-- ----------------------------------------------------------

CREATE VIEW v_content AS
SELECT m.id AS masterFileID
    ,c.versionID
    ,v.versionTimestamp
    ,m.url
    ,m.urlHash
    ,v.mimeType
    ,c.pageContent
FROM fileContent AS c
LEFT JOIN fileVersions AS v
ON c.versionID = v.id
LEFT JOIN fileMaster as m
on c.versionID = m.id;

-- ----------------------------------------------------------
-- STORED PROCEDURES
-- ----------------------------------------------------------

DELIMITER $$

CREATE PROCEDURE delete_all_versions_SP (IN fileMasterID_p INT)
MODIFIES SQL DATA
BEGIN
-- TO DO: DOES NOT DELETE THE ACTUAL FILES
DECLARE EXIT HANDLER FOR sqlexception
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;
    -- remove all labels attached to versions:
    DELETE FROM labelToVersion WHERE versionID IN (
        SELECT id FROM fileVersions WHERE fileID = filemasterID_p
        );
    -- now as the CONSTRAINT does not interfere, remove all versions:
    DELETE FROM fileVersions WHERE fileMasterID = fileMasterID_p;

    -- remove all labels attached to the fileMaster:
    DELETE FROM labelToMaster WHERE masterID = fileMasterID_p;
    -- now as there are no versions and the label CONSTRAINT
    -- does not interfere, remove the entry in FileMaster:
    DELETE FROM fileMaster WHERE id = fileMasterID_p;
    COMMIT;

END $$



DELIMITER $$
CREATE PROCEDURE transfer_labels_from_queue_to_master_SP (IN queueID_p CHAR(32) CHARACTER SET ASCII,
                                                          IN masterID_p INT)
MODIFIES SQL DATA
BEGIN
-- Not a transaction as designed to be called from within transactions.

    CREATE TEMPORARY TABLE foundLabels_tmp (
        SELECT labelID FROM labelToQueue WHERE queueID = queueID_p
    );

    -- Transfer labels if they exist:
    SELECT COUNT(*) FROM foundLabels_tmp INTO @numLabels;
    IF @numLabels > 0 THEN
        INSERT IGNORE INTO labelToMaster (labelID, masterID)
            SELECT labelID, masterID_p AS masterID FROM foundLabels_tmp;

    END IF;

    -- If there were labels attached to the queue item, remove them.
    -- Then remove the queue-entry:
    CALL delete_from_queue_SP (queueID_p);

    -- Drop the temporary table as we sustain the db connection:
    DROP TEMPORARY TABLE foundLabels_tmp;

END $$



DELIMITER $$
CREATE PROCEDURE delete_from_queue_SP (IN queueID_p CHAR(32) CHARACTER SET ASCII)
MODIFIES SQL DATA
BEGIN
-- Not a transaction as designed to be called from within transactions.
-- If there were labels attached to the queue item, remove them.
-- Then remove the queue-entry.


    -- remove all labels attached to the queue item:
    DELETE FROM labelToQueue WHERE queueID = queueID_p;
    -- now as the constraint does not interfere, remove the queue-entry:
    DELETE FROM queue WHERE id = queueID_p;

END $$



DELIMITER $$
CREATE PROCEDURE insert_file_SP (IN url_p TEXT,
                                 IN url_hash_p CHAR(64),
                                 IN queueID_p CHAR(32) CHARACTER SET ASCII,
                                 IN mimeType_p VARCHAR(127),
                                 IN path_or_bucket_p VARCHAR(2048),
                                 IN file_name_p VARCHAR(255),
                                 IN size_p INT UNSIGNED,
                                 IN hash_method_p VARCHAR(6),
                                 IN hash_value_p VARCHAR(512)
                                 )
MODIFIES SQL DATA
BEGIN

DECLARE EXIT HANDLER FOR sqlexception
    BEGIN
        ROLLBACK;
        UPDATE queue SET causesError = 2 WHERE id = queueID_p;
        RESIGNAL;
    END;

    START TRANSACTION;

    INSERT INTO fileMaster (url, urlHash) VALUES (url_p, url_hash_p);

    SELECT id FROM fileMaster WHERE urlHash = url_hash_p INTO @fileMasterID;

    INSERT INTO fileVersions (id, fileMasterID, storageTypeID, mimeType,
                              pathOrBucket, fileName, size, hashMethod,
                              hashValue) VALUES (queueID_p, @fileMasterID, 2, mimeType_p,
                              path_or_bucket_p, file_name_p, size_p,
                              hash_method_p, hash_value_p);

    CALL transfer_labels_from_queue_to_master_SP(queueID_p, @fileMasterID);

    COMMIT;

END $$



-- --------------------------------------------------------
-- insert_content_SP:
-- Saves the content, transfers the labels from the queue,
-- and removes the queue item.
--
-- --------------------------------------------------------
DELIMITER $$
CREATE PROCEDURE insert_content_SP (IN url_p TEXT,
                                    IN url_hash_p CHAR(64),
                                    IN queueID_p CHAR(32) CHARACTER SET ASCII,
                                    IN mimeType_p VARCHAR(127),
                                    IN text_p MEDIUMTEXT)
MODIFIES SQL DATA
BEGIN

DECLARE EXIT HANDLER FOR sqlexception
    BEGIN
        ROLLBACK;
        UPDATE queue SET causesError = 2 WHERE id = queueID_p;
        RESIGNAL;
    END;

    START TRANSACTION;

    INSERT INTO fileMaster (url, urlHash) VALUES (url_p, url_hash_p);

    -- LAST_INSERT_ID() in MySQL / MariaDB is on connection basis!
    -- https://dev.mysql.com/doc/refman/8.0/en/getting-unique-id.html
    -- However, it seems unreliable.
    --
    -- As of December 2019 "INSERT ... RETURNING" is a feature in the
    -- current ALPHA version of MariaDB.
    -- Until that version is in use, an extra roundtrip is justified:
    SELECT id FROM fileMaster WHERE urlHash = url_hash_p INTO @fileMasterID;

    INSERT INTO fileVersions (id, fileMasterID, storageTypeID, mimeType)
    VALUES (queueID_p, @fileMasterID, 1, mimeType_p);

    INSERT INTO fileContent (versionID, pageContent)
    VALUES (queueID_p, text_p);

    CALL transfer_labels_from_queue_to_master_SP(queueID_p, @fileMasterID);

    COMMIT;
END $$
DELIMITER ;



-- --------------------------------------------------------
-- next_queue_object_SP:
-- A stored procedure to return the next object in the queue.
-- It does not return URLs which are temoprarily blocked or
-- cause errors.
-- --------------------------------------------------------
DELIMITER $$
CREATE PROCEDURE next_queue_object_SP ()
NOT DETERMINISTIC
READS SQL DATA
BEGIN

    SELECT
    id
    ,action
    ,url
    ,urlHash
    ,prettifyHtml
    FROM queue
    WHERE causesError IS NULL AND
    (delayUntil IS NULL OR delayUntil < NOW())
    ORDER BY addedToQueue ASC
    LIMIT 1;

END$$
DELIMITER ;


CREATE FUNCTION num_items_with_temporary_errors ()
-- Some errors are only temporary. So before the bot stops,
-- those have to be counted.
RETURNS INTEGER
RETURN(SELECT COUNT(*) FROM queue WHERE causesError IN (
    SELECT id FROM exo.errorType WHERE permanent = 0)
    );

CREATE FUNCTION num_items_with_permanent_error ()
-- Count the number of errors the bot cannot resolve.
RETURNS INTEGER
RETURN(SELECT COUNT(*) FROM queue WHERE causesError IN (
    SELECT id FROM exo.errorType WHERE permanent = 1)
    );