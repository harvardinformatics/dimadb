#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
loader.py  Command line interface for dimadb

Created on  2017-03-31 14:52:41

@author: akitzmiller
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''
import sys, os, traceback
import logging
from dimadb import __version__ as version
from dimadb import Store


from argparse import ArgumentParser, RawDescriptionHelpFormatter


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


def initArgs():
    '''
    Setup arguments with parameterdef, check envs, parse commandline, return args
    '''

    parameterdefs = [
        {
            'name'      : 'DIMADB_LOGLEVEL',
            'switches'  : ['--loglevel'],
            'required'  : False,
            'help'      : 'Log level (e.g. DEBUG, INFO)',
            'default'   : 'ERROR',
        },
        {
            'name'      : 'DIMADB_DRIVER',
            'switches'  : ['--driver'],
            'required'  : False,
            'help'      : 'Database connection driver (e.g. mysql+mysqldb).  See SQLAlchemy docs.',
            'default'   : 'mysql+mysqldb',
        },
        {
            'name'      : 'DIMADB_USER',
            'switches'  : ['--user'],
            'required'  : False,
            'help'      : 'Database user',            
        },
        {
            'name'      : 'DIMADB_PASSWORD',
            'switches'  : ['--password'],
            'required'  : False,
            'help'      : 'Database password',
        },
        {
            'name'      : 'DIMADB_HOST',
            'switches'  : ['--host'],
            'required'  : False,
            'help'      : 'Database hostname',
        },
        {
            'name'      : 'DIMADB_DATABASE',
            'switches'  : ['--database'],
            'required'  : False,
            'help'      : 'Database name',
        },
        {
            'name'      : 'DIMADB_DATASET',
            'switches'  : ['--dataset'],
            'required'  : False,
            'help'      : '''A name for the current data load.  Can be useful for accessing particular datasets later on.''',
        },
    ]
        
    # Check for environment variable values
    # Set to 'default' if they are found
    for parameterdef in parameterdefs:
        if os.environ.get(parameterdef['name'],None) is not None:
            parameterdef['default'] = os.environ.get(parameterdef['name'])
            
    # Setup argument parser
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-V', '--version', action='version', version=version)
    parser.add_argument('FILE',help='Input data file')
    
    # Use the parameterdefs for the ArgumentParser
    for parameterdef in parameterdefs:
        switches = parameterdef.pop('switches')
        if not isinstance(switches, list):
            switches = [switches]
            
        # Gotta take it off for add_argument
        name = parameterdef.pop('name')
        parameterdef['dest'] = name
        if 'default' in parameterdef:
            parameterdef['help'] += '  [default: %s]' % parameterdef['default']
        parser.add_argument(*switches,**parameterdef)
        
        # Gotta put it back on for later
        parameterdef['name'] = name
        
    args = parser.parse_args()
    return args


def main():
    args = initArgs()

    dataset = None
    if args.DIMADB_DATASET:
        dataset = args.DIMADB_DATASET

    try:
        store = Store('%s://%s:%s@%s/%s' % (
            args.DIMADB_DRIVER, 
            args.DIMADB_USER, 
            args.DIMADB_PASSWORD, 
            args.DIMADB_HOST, 
            args.DIMADB_DATABASE,
        ))
        loadPeptideDataFile(store,args.FILE,dataset)

    except Exception as e:
        print '%s:\n%s' % (str(e), traceback.format_exc())
        return 1

if __name__ == '__main__':
    sys.exit(main())
