#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
dimadb.py  Command line interface for dimadb

Created on  2017-03-31 14:52:41

@author: akitzmiller
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''
import sys, os
import logging 

logging.basicConfig(format='%(asctime)s: %(message)s',level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ.get('DIMADB_LOGLEVEL','DEBUG')))


def loadPeptideDataFile(store, filename, dataset=None):
    '''
    Load standard peptide data file
    '''
    if not os.path.exists(filename):
        raise Exception('File %s does not exist.' % filename)

    if dataset is None:
        dataset = os.path.basename(filename)

    headers = []
    with open(filename,'r') as f:
        peptidedata = {}
        for line in f:
            line = line.strip()
            if line == '':
                continue
            if len(headers) == 0:
                headers = line.split('\t')
                continue

            peptidedata = dict(zip(headers,line.split('\t')))
            if 'dataset' not in peptidedata:
                peptidedata['dataset'] = dataset

            store.savePeptideData(peptidedata)


def main():
    pass


if __name__ == '__main__':
    sys.exit(main())
