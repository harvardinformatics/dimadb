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
SEQS_FILE = 'seqs.txt'
PEPTIDE_DATA = '''Confidence  Annotated Sequence  Modifications   Modifications in Master Proteins    # Protein Groups    # Proteins  # PSMs  Master Protein Accessions   Positions in Master Proteins    # Missed Cleavages  Theo. MH+ [Da]  Abundance Ratio: (127N) / (126) Abundance Ratio: (127C) / (126) Abundance Ratio: (128N) / (126) Abundance Ratio: (128C) / (126) Abundance Ratio: (129C) / (129N)    Abundance Ratio: (130N) / (129N)    Abundance Ratio: (130C) / (129N)    Abundance Ratio: (131) / (129N) Abundances (Grouped): 126   Abundances (Grouped): 127N  Abundances (Grouped): 127C  Abundances (Grouped): 128N  Abundances (Grouped): 128C  Abundances (Grouped): 129N  Abundances (Grouped): 129C  Abundances (Grouped): 130N  Abundances (Grouped): 130C  Abundances (Grouped): 131   Contaminant Off by X    Position in Protein Confidence (by Search Engine): A9 PMI-Byonic    Confidence (by Search Engine): Sequest HT   Percolator q-Value (by Search Engine): Sequest HT   Percolator PEP (by Search Engine): Sequest HT   Percolator SVMScore (by Search Engine): Sequest HT
High    [-].MAMSAASDGNHVAPPELMGQSPPHSPR.[A] 2xPhospho [S21; S25]; 2xOxidation [M3; M18]; 1xTMT6plex [N-Term]    F4JLS6 2xPhospho [S21; S25] 1   1   1   F4JLS6  F4JLS6 [1-27]   0   3193.35049  0.83    1.27    1.33    0.97    0.87    0.98    0.82    1.50    7.90E+03    6.58E+03    1.01E+04    1.05E+04    7.67E+03    6.98E+03    6.05E+03    6.86E+03    5.74E+03    1.05E+04    FALSE   0   0   n/a High    0.001511    0.01686 0.345
High    [-].MAPQLLSSSNFK.[S]    1xAcetyl [N-Term]; 2xDeamidated [Q4; N10]; 1xPhospho [S7]; 1xTMT6plex [K/M] Q8GWU0 1xAcetyl [N-Term]; 1xPhospho [S7]    1   1   1   Q8GWU0  Q8GWU0 [1-12]   0   1904.94809  0.87    1.17    1.11    0.81    0.66    0.88    0.76    0.91    1.63E+04    1.42E+04    1.91E+04    1.82E+04    1.32E+04    1.36E+04    9.03E+03    1.19E+04    1.03E+04    1.24E+04    FALSE   0   0   n/a High    0.003709    0.05089 0.168
High    [-].MASPRVVSEDRK.[S]    1xPhospho [S8]; 1xTMT6plex [K/M]    F4IE60 1xPhospho [S8]   1   2   2   F4IE60  F4IE60 [1-12]   2   1913.008    1.11    1.31    1.23    1.16    0.76    0.89    0.96    1.32    8.02E+03    8.93E+03    1.05E+04    9.82E+03    9.32E+03    7.52E+03    5.72E+03    6.71E+03    7.22E+03    9.96E+03    FALSE   0   0   n/a High    0.007352    0.1213  0.046
High    [-].MDSIQQR.[R] 1xTMT6plex [N-Term]; 1xPhospho [S3(100)]    Q9LZU5 1xPhospho [S3(100)]  1   1   5   Q9LZU5  Q9LZU5 [1-7]    0   1186.54889  0.78    1.09    1.72    1.11    0.71    0.81    1.02    1.12    3.86E+04    3.01E+04    4.21E+04    6.62E+04    4.27E+04    2.94E+04    2.09E+04    2.39E+04    2.99E+04    3.29E+04    FALSE   0   1   High    High    0.006477    0.1051  0.065
'''
PEPTIDE_DATA_FILE = 'test.txt'


class Test(unittest):

    def setUp(self):
        try:
            os.unlink(PEPTIDE_DATA_FILE)
        except Exception:
            pass

        with open(PEPTIDE_DATA_FILE,'w') as f:
            f.write(PEPTIDE_DATA)

    def tearDown(self):
        try:
            os.unlink(PEPTIDE_DATA_FILE)
        except Exception:
            pass

    def testPeptideLoad(self):
        '''
        Test loadPeptideDataFile function
        '''
        store = Store(DIMADB_CONNECT)
        loadPeptideDataFile(store, PEPTIDE_DATA_FILE)
