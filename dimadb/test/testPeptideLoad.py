# -*- coding: utf-8 -*-

'''
test of peptide data load

Created on  2017-03-31 14:50:26

@author: akitzmiller
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''

import unittest, os
import subprocess
from BioSQL import BioSeqDatabase
from sqlalchemy.engine import create_engine
from sqlalchemy import select,func
from dimadb import loadPeptideDataFile, Store

DIMADB_TEST_DRIVER = os.environ.get('DIMADB_TEST_DRIVER', 'mysql+mysqldb')
DIMADB_TEST_USER = os.environ.get('DIMADB_TEST_USER')
DIMADB_TEST_PASSWORD = os.environ.get('DIMADB_TEST_PASSWORD')
DIMADB_TEST_DATABASE = os.environ.get('DIMADB_TEST_DATABASE','dimadbtest')
DIMADB_TEST_HOST = os.environ.get('DIMADB_TEST_HOST')

SEQS_FILE = 'seqs.txt'
PEPTIDE_DATA_FILE = os.path.join(os.path.dirname(__file__),'partialSpreadforTesting.txt')

BIOSQL_NAMESPACE = 'test'

for key in ['SEQDB_LOADER_USER', 'SEQDB_LOADER_PASSWORD', 'SEQDB_LOADER_HOST', 'SEQDB_LOADER_DATABASE']:
    if not os.environ.get(key) or os.environ[key] == '':
        raise Exception('Must set SEQDB_LOADER_{USER,PASSWORD,HOST,DATABASE}')


def runcmd(cmd):
    '''
    Execute a command and return stdout, stderr, and the return code
    '''
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutstr, stderrstr = proc.communicate()
    return (proc.returncode, stdoutstr, stderrstr)


def loadseqs():
    '''
    Load sequences
    '''
    server = BioSeqDatabase.open_database(
        driver=os.environ.get('SEQDB_LOADER_DRIVER', 'MySQLdb'),
        user=os.environ.get('SEQDB_LOADER_USER'),
        passwd=os.environ.get('SEQDB_LOADER_PASSWORD'),
        host=os.environ.get('SEQDB_LOADER_HOST'),
        db=os.environ.get('SEQDB_LOADER_DATABASE'),
    )
    try:
        del server[BIOSQL_NAMESPACE]
    except Exception:
        pass

    server.new_database(BIOSQL_NAMESPACE)
    server.commit()
    cmd = 'seqdb-loader --namespace %s --parser uniprox-xml sample.xml' % BIOSQL_NAMESPACE
    returncode, stdout, stderr = runcmd(cmd)
    if returncode != 0:
        raise Exception('seqdb-loader failed: %s' % stderr)


def removeseqs():
    '''
    Remove sequences
    '''

    server = BioSeqDatabase.open_database(
        driver=os.environ.get('SEQDB_LOADER_DRIVER', 'MySQLdb'),
        user=os.environ.get('SEQDB_LOADER_USER'),
        passwd=os.environ.get('SEQDB_LOADER_PASSWORD'),
        host=os.environ.get('SEQDB_LOADER_HOST'),
        db=os.environ.get('SEQDB_LOADER_DATABASE'),
    )
    try:
        del server[BIOSQL_NAMESPACE]
    except Exception:
        pass


def initdb():
    engine = create_engine(
        '%s://%s:%s@%s' % (DIMADB_TEST_DRIVER, DIMADB_TEST_USER, DIMADB_TEST_PASSWORD, DIMADB_TEST_HOST))
    connection = engine.connect()
    try:
        connection.execute('drop database %s' % DIMADB_TEST_DATABASE)
    except Exception:
        pass

    connection.execute('create database %s' % DIMADB_TEST_DATABASE)
    engine.dispose()


def destroydb():
    engine = create_engine(
        '%s://%s:%s@%s' % (DIMADB_TEST_DRIVER, DIMADB_TEST_USER, DIMADB_TEST_PASSWORD, DIMADB_TEST_HOST))
    connection = engine.connect()
    try:
        connection.execute('drop database %s' % DIMADB_TEST_DATABASE)
    except Exception:
        pass
    engine.dispose()


class Test(unittest.TestCase):

    def setUp(self):
        try:
            destroydb()
        except Exception:
            pass
        initdb()
        self.store = Store('%s://%s:%s@%s/%s' % (DIMADB_TEST_DRIVER, DIMADB_TEST_USER, DIMADB_TEST_PASSWORD, DIMADB_TEST_HOST, DIMADB_TEST_DATABASE))
        self.store.create()

#    def tearDown(self):
#
#        del self.store
#        destroydb()

    def testPeptideLoad(self):
        '''
        Test loadPeptideDataFile function
        '''
        loadPeptideDataFile(self.store, PEPTIDE_DATA_FILE)
        s = select([func.count(self.store.tables['peptide'].c.id)])
        rs = s.execute()
        peptidecount = rs.first()[0]
        self.assertTrue(38 == peptidecount,'Incorrect peptide count %d' % peptidecount)
