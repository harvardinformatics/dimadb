# -*- coding: utf-8 -*-

'''
test of peptide data load

Created on  2017-03-31 14:50:26

@author: akitzmiller
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''

import unittest, os
from dimadb import loadPeptideDataFile, Store

DIMADB_CONNECT = os.environ.get('DIMADB_CONNECT')
PEPTIDE_DATA_FILE = 'partialSpreadforTesting.txt'
SEQS_FILE = 'seqs.txt'


class Test(unittest):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testPeptideLoad(self):
        '''
        Test loadPeptideDataFile function
        '''
        store = Store(DIMADB_CONNECT)
        loadPeptideDataFile(store, PEPTIDE_DATA_FILE)
