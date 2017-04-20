# -*- coding: utf-8 -*-

'''
dimadb.store - Database interaction class for dimadb

Created on  2017-03-31 12:26:37

@author: akitzmiller
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''
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

    def __init__(self,connectstring):
        '''
        Create the engine and connection.  Define the jobreport table
        '''

        # configure Session class with desired options
        self.engine = create_engine(connectstring)
        Session = sessionmaker(bind=self.engine)

        self.session = Session()
        self.metadata = MetaData()
        
        self.tables = {}
        self.tables['seq'] = Table(
            'seq',
            self.metadata,
            Column('id',                            types.Integer,      primary_key=True, autoincrement='auto'),
            Column('accession',                     types.String(50),   nullable=False),
            Column('version',                       types.Integer),
            Column('authority',                     types.String(20),   nullable=False),
            Column('organism',                      types.String(100),  nullable=False),
        )
        self.tables['peptide'] = Table(
            'peptide', 
            self.metadata, 
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('confidence',                    types.String(10)),
            Column('annotated_sequence',            types.String(100), nullable=False),
            Column('modifications',                 types.String(1000), nullable=False),
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
            Column('peptide_id',                   types.Integer, ForeignKey('peptide.id'),nullable=False),
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
            Column('seq_id',                        types.Integer, nullable=True),
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

        self.storeMasterProteins(peptidedata)
        peptide = self.storePeptide(peptidedata)

        for methodname in PEPTIDE_DATA_METHODS:
            f = getattr(self,methodname)
            f(peptide, peptidedata)

    def storeMasterProteins(self,peptidedata):
        '''
        Extracts master protein references from the peptide data record. 
        If the protein is not already in the seqs table, fetches from Uniprot and adds it via storeSeqs
        '''
        pass

    def storeSeqs(self,**kwargs):
        '''
        Saves a seq record
        '''
        pass

    def storePeptide(self,peptidedata):
        '''
        Takes the dictionary of peptide data and makes a peptide record from it
        '''
        # Clean out the empties so that None can be null
        pdata = dict((k,v) for k,v in peptidedata.iteritems() if v != '')

        i = self.tables['peptide'].insert()
        i.execute(
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

    def storePeptideAbundances(self,peptide,peptidedata):
        '''
        Takes the dictionary of peptide data and makes an abundance record from it
        '''
        pass

    def storePeptideSeqMatches(self,peptide,peptidedata):
        '''
        Take the dictionary of peptide data and makes seq match records
        '''
        pass

    def storePeptideModifications(self,peptide,peptidedata):
        '''
        Take the dictionary of peptide data and make peptide modification records
        '''
        pass
