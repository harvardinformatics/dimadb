# -*- coding: utf-8 -*-

'''
dimadb.store - Database interaction class for dimadb

Created on  2017-03-31 12:26:37

@author: akitzmiller
@copyright: 2016 The Presidents and Fellows of Harvard College. All rights reserved.
@license: GPL v2.0
'''
from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData, Column, Table, types
from sqlalchemy.orm import sessionmaker


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
            Column('off_by_x',                      types.Integer),
            Column('position_in_protein',           types.Integer),
        )

        self.tables['peptide_modification'] = Table(
            'peptide_modification',
            self.metadata,
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('peptide_id'),                   types.ForeignKey('peptide.id',nullable=False),
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
            Column('peptide_id',                    types.ForeignKey('peptide.id'), nullable=False),
            Column('seq_id',                        types.ForeignKey('seq.id'), nullable=False),
        )

        self.tables['peptide_seq_match_data'] = Table(
            'peptide_seq_match_data',
            self.metadata,
            Column('id',                            types.Integer, primary_key=True, autoincrement='auto'),
            Column('peptide_seq_match_id',          types.ForeignKey('peptide_seq_match'), nullable=False),
            Column('name',                          types.String(100), nullable=False),
            Column('strval',                        types.String(100), nullable=False),
            Column('fval',                          types.Float),
        )

        self.metadata.bind = self.engine
        self.connection = self.engine.connect()

    def create(self):
        """
        Actually creates the database tables.  Be careful
        """
        self.metadata.create_all(checkfirst=True)
        
    def drop(self):
        """
        Drop the database table
        """
        self.metadata.drop_all(checkfirst=True)
