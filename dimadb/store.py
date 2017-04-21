# -*- coding: utf-8 -*-

'''
dimadb.store - Database interaction class for dimadb

Created on  2017-03-31 12:26:37

@author: akitzmiller
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''
import re

from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData, Column, Table, types, ForeignKey
from sqlalchemy.orm import sessionmaker

# List of functions on the Store class that process peptide data.  Order is important.
# Run by the savePeptideData function
PEPTIDE_DATA_METHODS = [
    'storePeptideAbundances',
    'storePeptideSeqMatches',
    'storePeptideModifications',
]


class Store(object):
    '''
    Class that manages interactions with the database
    '''

    def __init__(self,connectstring=None):
        '''
        Create the engine and connection.  Define the jobreport table
        '''

        if connectstring is None:
            connectstring = '%s//%s:%s@%s'

        # configure Session class with desired options
        self.engine = create_engine(connectstring)
        Session = sessionmaker(bind=self.engine)

        self.session = Session()
        self.metadata = MetaData()
        
        self.tables = {}
        self.tables['peptide'] = Table(
            'peptide', 
            self.metadata, 
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('confidence',                    types.String(10)),
            Column('annotated_sequence',            types.String(100)),
            Column('modifications',                 types.String(1000)),
            Column('modifications_in_master_proteins',  types.String(1000)),
            Column('no_protein_groups',             types.Integer),
            Column('no_proteins',                   types.Integer),
            Column('no_psms',                       types.Integer),
            Column('master_protein_accessions',         types.String(100)),
            Column('positions_in_master_proteins',      types.String(1000)),
            Column('no_missed_cleavages',           types.Integer),
            Column('theo_mh_da',                    types.Float),
            Column('contaminant',                   types.Boolean, nullable=False, default=False),
            Column('off_by_x',                      types.Integer, nullable=True),
            Column('position_in_protein',           types.Integer, nullable=True),
            Column('dataset',                       types.String(200), nullable=False)
        )

        self.tables['peptide_modification'] = Table(
            'peptide_modification',
            self.metadata,
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('peptide_id',                    types.Integer, ForeignKey('peptide.id'),nullable=False),
            Column('mod_type',                      types.String(50), nullable=False),
            Column('loc_base',                      types.String(1)),
            Column('loc_pos',                       types.Integer),
            Column('loc_str',                       types.String(50), nullable=False),
        )

        self.tables['peptide_seq_match'] = Table(
            'peptide_seq_match',
            self.metadata,
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('confidence',                    types.String(10)),
            Column('algorithm',                     types.String(100)),
            Column('peptide_id',                    types.Integer, ForeignKey('peptide.id'), nullable=False),
            Column('seq_id',                        types.String(100)),
            Column('start',                         types.Integer),
            Column('end',                           types.Integer),
        )

        self.tables['peptide_seq_match_data'] = Table(
            'peptide_seq_match_data',
            self.metadata,
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('peptide_seq_match_id',          types.Integer, ForeignKey('peptide_seq_match.id'), nullable=False),
            Column('name',                          types.String(100), nullable=False),
            Column('strval',                        types.String(100), nullable=False),
            Column('fval',                          types.Float),
        )
        self.tables['peptide_abundance'] = Table(
            'peptide_abundance',
            self.metadata,
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('peptide_id',                    types.Integer, ForeignKey('peptide.id'), nullable=False),
            Column('name',                          types.String(10), nullable=False),
            Column('val',                           types.Integer, nullable=False),            
        )

        self.metadata.bind = self.engine
        self.connection = self.engine.connect()

    def create(self):
        '''
        Actually creates the database tables.  Be careful
        '''
        self.metadata.create_all(checkfirst=True)
        
    def drop(self):
        '''
        Drop the database table
        '''
        self.metadata.drop_all(checkfirst=True)

    def savePeptideData(self,peptidedata):
        '''
        Takes a dictionary of peptide data and saves it to the db.
        Right now this is the proteomics search output from Juerg.

        Iterates through all of the functions in the PEPTIDE_DATA_METHODS list.
        '''

        peptide_id = self.storePeptide(peptidedata)

        for methodname in PEPTIDE_DATA_METHODS:
            f = getattr(self,methodname)
            f(peptide_id, peptidedata)

    def storePeptide(self,peptidedata):
        '''
        Takes the dictionary of peptide data and makes a peptide record from it
        '''
        # Clean out the empties so that None can be null
        pdata = dict((k,v) for k,v in peptidedata.iteritems() if v != '')

        i = self.tables['peptide'].insert()
        rs = i.execute(
            confidence=pdata.get('Confidence'),
            annotated_sequence=pdata.get('Annotated Sequence'),
            modifications=pdata.get('Modifications'),
            modifications_in_master_proteins=pdata.get('Modifications in Master Proteins'),
            no_protein_groups=pdata.get('# Protein Groups'),
            no_proteins=pdata.get('# Proteins'),
            no_psms=pdata.get('# PSMs'),
            master_protein_accessions=pdata.get('Master Protein Accessions'),
            positions_in_master_proteins=pdata.get('Positions in Master Proteins'),
            no_missed_cleavages=pdata.get('# Missed Cleavages'),
            theo_mh_da=pdata.get('Theo. MH+ [Da]'),
            contaminant=pdata.get('Contaminant') == 'TRUE',
            off_by_x=pdata.get('Off by X'),
            position_in_protein=pdata.get('Position in Protein'),
        )
        self.session.commit()
        return rs.lastrowid

    def storePeptideAbundances(self,peptide_id,peptidedata):
        '''
        Takes the dictionary of peptide data and makes an abundance record from it
        '''
        pdata = dict((k,v) for k,v in peptidedata.iteritems() if v != '')
        headerre = re.compile(r'Abundances \(Grouped\): ([^ ]+)')
        for k,v in pdata.iteritems():
            m = headerre.match(k)
            if m:
                name = m.group(1)
                val = int(v)
                i = self.tables['peptide_abundance'].insert()
                i.execute(
                    peptide_id=peptide_id,
                    name=name,
                    val=val,
                )
        self.session.commit()

    def storePeptideSeqMatches(self,peptide_id,peptidedata):
        '''
        Take the dictionary of peptide data and makes seq match records
        '''
        pdata = dict((k,v) for k,v in peptidedata.iteritems() if v != '')
        masterstr = pdata.get('Positions in Master Proteins')
        confidence = pdata.get('Confidence')

        # Capture additional data for peptide_seq_match_data
        matchdata = []
        for k,v in pdata.iteritems():
            if '(by Search Engine)' in k:
                d = {
                    'name' : k,
                    'strval' : v,
                }
                try:
                    fval = float(v)
                    d['fval'] = fval
                except ValueError:
                    pass
                matchdata.append(d)

        posre = re.compile(r'([^ ]+) \[(\d+)-(\d+)\]')
        if masterstr is not None and masterstr.strip() != '':
            mastermatches = re.split(r'\s*;\s*',masterstr)
            for mastermatch in mastermatches:
                m = posre.match(mastermatch)
                if m is None:
                    raise Exception('Master protein string does not look right: %s' % mastermatch)
                acc = m.group(1)
                start = int(m.group(2))
                end = int(m.group(3))
                i = self.tables['peptide_seq_match'].insert()
                rs = i.execute(
                    peptide_id=peptide_id,
                    seq_id=acc,
                    start=start,
                    end=end,
                    confidence=confidence,
                )
                peptide_seq_match_id = rs.lastrowid
                self.session.commit()

                i = self.tables['peptide_seq_match_data'].insert()
                for d in matchdata:
                    d['peptide_seq_match_id'] = peptide_seq_match_id
                    i.execute(**d)

                self.session.commit()

    def storePeptideModifications(self,peptide_id,peptidedata):
        '''
        Take the dictionary of peptide data and make peptide modification records
        '''
        # Clean out the empties so that None can be null
        pdata = dict((k,v) for k,v in peptidedata.iteritems() if v != '')

        modifications = re.split('r\s*;\s*',pdata.get('Modifications',''))

        # Modification string matcher, e.g. 2xPhospho [S21; S25] or TMT6plex [K]
        modre = re.compile(r'\d*x*([^\s]+) \[([^\]]+)\]')

        # Location string matcher, e.g. S21
        modlocre = re.compile(r'([A-Z])(\d+)')

        for modstr in modifications:
            m = modre.match(modstr)
            if m is None:
                raise Exception('Modification string makes no sense: %s' % modstr)

            mod_type = m.group(1).lower()
            mod_locstr = m.group(2)
            mod_locs = re.split(r'\s*;\s*',mod_locstr)

            # Iterate over what may be more than one location
            for mod_loc in mod_locs:
                loc_base = None
                loc_pos = None
                m = modlocre.match(mod_loc)
                if m is not None:
                    loc_base = m.group(1)
                    loc_pos = m.group(2)
                i = self.tables['peptide_modification'].insert()
                i.execute(
                    peptide_id=peptide_id,
                    mod_type=mod_type,
                    loc_base=loc_base,
                    loc_pos=loc_pos,
                    loc_str=mod_loc,
                )
        self.session.commit()
