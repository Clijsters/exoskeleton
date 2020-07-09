#!/usr/bin/env python3
# -*- coding: utf-8 -*-

u""" Database class encapsulating database functionalities """

import logging
import pymysql
import userprovided


class Database:

    def __init__(self, host):
        self.db_name = self.db_username = self.db_passphrase = None
        self.db_port = 3306
        self.db_autocommit: bool = True
        self.connection: pymysql.Connection = None
        if not host:
            logging.warning('No hostname provided. Will try localhost.')
            host = 'localhost'
        self.db_host = host

    def port(self, port):
        # TODO: port_in_range does a None check. Simplify.
        if not port:
            logging.info('No port number supplied. ' +
                         'Will try standard port instead.')
            port = 3306
        elif not userprovided.port.port_in_range(port):
            raise ValueError('Port outside valid range!')
        self.db_port = port
        return self

    def database(self, database):
        if not database:
            raise ValueError('You must provide the name of the database.')
        self.db_name = database
        return self

    def username(self, username):
        if not username:
            raise ValueError('You must provide a database user.')
        self.db_username = username
        return self

    def password(self, password):
        if not password or password == '':
            logging.warning('No database passphrase provided. ' +
                            'Will try to connect without.')
        self.db_passphrase = password
        return self

    def connect(self):
        if not self._is_ready():
            raise ValueError('Ensure all mandatory connection properties are set!')
        self.connection: pymysql.Connection = self._establish_db_connection()
        logging.info('Established database connection.')
        return self.connection

    def check_schema_compatibility(self):
        self._check_table_existence()
        self._check_stored_procedures()

    def _is_ready(self):
        return '' in [self.db_host, self.db_name, self.db_username]

    def _establish_db_connection(self) -> pymysql.Connection:
        u"""Establish a connection to MariaDB """
        try:
            logging.debug('Trying to connect to database.')
            return pymysql.connect(host=self.db_host,
                                   port=self.db_port,
                                   database=self.db_name,
                                   user=self.db_username,
                                   password=self.db_passphrase,
                                   autocommit=True)

        except pymysql.InterfaceError:
            logging.exception('Exception related to the database ' +
                              '*interface*.', exc_info=True)
            raise
        except pymysql.DatabaseError:
            logging.exception('Exception related to the database.',
                              exc_info=True)
            raise
        except Exception:
            logging.exception('Unknown exception while ' +
                              'trying to connect to the DBMS.',
                              exc_info=True)
            raise

    def _check_table_existence(self) -> bool:
        u"""Check if all expected tables exist."""
        logging.debug('Checking if the database table structure is complete.')
        cursor = self.connection.cursor()
        expected_tables = ['actions',
                           'blockList',  # TODO: Shouldn't that be `blockList`?
                           'errorType',
                           'fileContent',
                           'fileMaster',
                           'fileVersions',
                           'jobs',
                           'labels',
                           'labelToMaster',
                           'labelToVersion',
                           'queue',
                           'statisticsHosts',
                           'storageTypes']
        tables_count = 0

        cursor.execute('SHOW TABLES;')
        tables = cursor.fetchall()
        if not tables:
            logging.error('The database exists, but no tables found!')
            raise OSError('Database table structure missing. ' +
                          'Run generator script!')
        else:
            tables_found = [item[0] for item in tables]
            for table in expected_tables:
                if table in tables_found:
                    tables_count += 1
                    logging.debug('Found table %s', table)
                else:
                    logging.error('Table %s not found.', table)

        cursor.close()
        if tables_count != len(expected_tables):
            raise RuntimeError('Database Schema Incomplete: Missing Tables!')

        logging.info("Found all expected tables.")
        return True

    def _check_stored_procedures(self) -> bool:
        u"""Check if all expected stored procedures exist and if the user
        is allowed to execute them. """
        logging.debug('Checking if stored procedures exist.')
        cursor = self.connection.cursor()
        expected_procedures = ['delete_all_versions_SP',
                               'delete_from_queue_SP',
                               'insert_content_SP',
                               'insert_file_SP',
                               'next_queue_object_SP']

        procedures_count = 0
        cursor.execute('SELECT SPECIFIC_NAME ' +
                       'FROM INFORMATION_SCHEMA.ROUTINES ' +
                       'WHERE ROUTINE_SCHEMA = %s;',
                       self.db_name)
        procedures = cursor.fetchall()
        procedures_found = [item[0] for item in procedures]
        for procedure in expected_procedures:
            if procedure in procedures_found:
                procedures_count += 1
                logging.debug('Found stored procedure %s', procedure)
            else:
                logging.error('Stored Procedure %s is missing (create it ' +
                              'with the database script) or the user lacks ' +
                              'permissions to use it.', procedure)
        cursor.close()
        if procedures_count != len(expected_procedures):
            raise RuntimeError('Database Schema Incomplete: ' +
                               'Missing Stored Procedures!')

        logging.info("Found all expected stored procedures.")
        return True
