import csv
import sqlite3
import pandas as pd
import operator
import io
import ast
import typing

class DatabaseInputError(Exception):
    def __init__(self, string):
        super().__init__(f"There are non-alphanumeric characters in '{string}' that are not allowed for identifiers in the database.")

class DatabaseReadError(Exception):
    def __init__(self, tablename):
        super().__init__(f"The table {tablename} does not exist.")

class DatabaseWriteError(Exception):
    def __init__(self, tablename):
        super().__init__(f"The table {tablename} already exists.")

class TableKeyError(Exception):
    def __init__(self):
        super().__init__(f"Type of index field is not specified.")
        
class SQLIdentifier:
    '''Handles values, e.g. parameters, that are given to the SQLite-database'''
    # TODO handle dictionaries and tuples as input as well
    
    def __init__(self, value: str) -> None:
        self.value = value

    def __repr__(self) -> str:
        '''Raise error if self.value includes any non-(alphanumerical or "_") character'''
        for char in self.value:
            if not char.isalnum() and not char == '_':
                raise DatabaseInputError(self.value)
        return self.value
        

class Database:
    '''Interacts with the SQLite-Database'''
    
    def __init__(self, filename: str) -> None:
        self.connection = sqlite3.connect(filename)
        self.cursor = self.connection.cursor()

    def __del__(self) -> None:
        '''Close database'''
        self.connection.close()

    def __setitem__(self, tablename: str, table: 'Table') -> None:
        '''Safes <table> in SQL-database with <tablename> (overwrite if table already exists)'''
        self._dropTable(tablename)
        self._createTable(tablename, table.getFields())
        for dataset in table.content:
            self._insertDatasetIntoTable(tablename, dataset)

    def __getitem__(self, tablename: str) -> None:
        '''Returns SQL-table with <tablename>'''
        return Table(
            indexField=self._getPrimaryKeyOfTable(tablename),
            content=self._getTable(tablename)
        )

    def __enter__(self) -> 'Database':
        return self

    def __exit__(self, exceptionType, exceptionValue, traceback) -> bool:
        '''Commits database changes if not exception is raised, otherwise rolls back changes'''
        if exceptionValue:
            self.connection.rollback()
            # if issubclass(exceptionType, KeyboardInterrupt):
            # print("Keyboard interrupt!")
            return False
        self.connection.commit()
        return True

    def _dropTable(self, tablename: str) -> None:
        '''Deletes table with name tablename if it exists'''
        self.cursor.execute(f'drop table if exists {SQLIdentifier(tablename)}')

    def _createTable(self, tablename: str, fieldtypes: typing.Dict[str, str]) -> None: 
        '''Creates tables with specified fieldtypes'''
        if self._hasTable(tablename):
            raise DatabaseWriteError(tablename)
        fieldheader = '(' + ', '.join(repr(SQLIdentifier(field)) + ' ' + fieldtypes[field] for field in fieldtypes) + ')'
        self.cursor.execute(f'create table {SQLIdentifier(tablename)} {fieldheader}')

    def _insertDatasetIntoTable(self, tablename: str, dataset): # typing.List[typing.Dict[str, (str, int, float)]]):
        '''Inserts one dataset into table'''
        # TODO check if fields exist in db-table
        fieldInput = tuple(SQLIdentifier(field) for field in dataset)
        valueInput = '(' + ', '.join('?' for a in tuple(dataset)) + ')'
        self.cursor.execute(f'insert into {SQLIdentifier(tablename)} {fieldInput} values {valueInput}', (tuple(dataset.values())))

    def _hasTable(self, tablename):
        '''Checks if table exists in db'''
        hasTable = self.cursor.execute(f'select count(*) from sqlite_master where type="table" and name="{SQLIdentifier(tablename)}"').fetchone()[0]
        return False if hasTable == 0 else True
        
    def _getTable(self, tablename):
        '''Returns full table as dataframe'''
        if not self._hasTable(tablename):
            raise DatabaseReadError(tablename)
        dataframe = pd.read_sql(f'select * from {SQLIdentifier(tablename)}', self.connection)
        return dataframe.to_dict(orient='records')

    def _getTableinfo(self, tablename):
        '''Returns table-information of table as dataframe'''
        if not self._hasTable(tablename):
            raise DatabaseReadError(tablename)
        dataframe = pd.read_sql(f'pragma table_info({SQLIdentifier(tablename)})', self.connection)
        return dataframe.to_dict(orient='records')

    def _getPrimaryKeyOfTable(self, tablename):
        '''Returns fieldname of primary key field in database-table <tablename>'''
        tableinfo = self._getTableinfo(tablename)
        for dataset in tableinfo:
            if dataset['pk'] == 1:
                return dataset['name']

            

class Table:
    def __init__(self, indexField, txtTypeFields=None, numTypeFields=None, content=None):
        self.indexedContent = {}
        self.indexField = indexField
        self.txtTypeFields = set(txtTypeFields or ())
        self.numTypeFields = set(numTypeFields or ())
        if not self.fields == set() and self.indexField not in self.fields:
            raise TableKeyError
        self._read(content or [])

    @classmethod
    def copy(cls, table):
        '''Copies <table> to this Table instance'''
        return(cls(
            indexField=''.join(table.indexField),
            txtTypeFields={field for field in table.txtTypeFields},
            numTypeFields={field for field in table.numTypeFields},
            content=[{k: v for k, v in x.items()} for x in table.content]
        ))

    @property
    def content(self):
        '''Table content in form of a list of datasets'''
        return list(self.indexedContent.values())

    @property
    def fields(self):
        '''All fieldnames'''
        return self.txtTypeFields | self.numTypeFields

    def __lshift__(self, table):
        '''Returns a tables that is this table, overwritte by <table>'''
        resultTable = Table.copy(self)
        for i in table.indexedContent:
            for columnToUpdate in table.fields:
                fieldType = str if columnToUpdate in table.txtTypeFields else int
                resultTable._addField(columnToUpdate, fieldType)
                if i in resultTable.indexedContent:
                    resultTable.indexedContent[i][columnToUpdate] = table.indexedContent[i][columnToUpdate]
                else:
                    resultTable.indexedContent[i] = table.indexedContent[i]
        return resultTable

    def __eq__(self, table):
        return (
            sorted(
                self.content,
                key=operator.itemgetter(self.indexField)
            ) == sorted(
                table.content,
                key=operator.itemgetter(self.indexField)
            )
        ) and (
            self.indexField == table.indexField
        ) and (
            self.txtTypeFields == table.txtTypeFields
        ) and (
            self.numTypeFields == table.numTypeFields
        )

    def __repr__(self):
        return str(self.content)

    def contentOf(self, keyvalue, columnname):
        '''Return content of field <columnname> in dataset with key <keyvalue> '''
        try:
            return self.indexedContent[keyvalue][columnname]
        except KeyError:
            return None

    def getFields(self):
        '''Returns all fields with fieldtypes'''
        fieldtypes = {}
        for field in self.fields:
            fieldtype = 'text' if field in self.txtTypeFields else 'numeric'
            if field == self.indexField:
                fieldtype += ' primary key'
            fieldtypes[field] = fieldtype
        return fieldtypes

    def _addField(self, field, fieldType):
        '''Add <field> to table with type <fieldType>'''
        if field not in self.fields:
            if issubclass(fieldType, str):
                self.txtTypeFields.add(field)
            else:
                assert issubclass(fieldType, (int, float)), (
                    'unexpected field type passed to addField'
                )
                self.numTypeFields.add(field)

    def _read(self, data):
        if isinstance(data, str):
            reader = csv.DictReader(data.splitlines(), delimiter='|', quotechar='"')
        elif isinstance(data, io.IOBase):
            reader = csv.DictReader(data, delimiter='|', quotechar='"')
        else:
            reader = data
        if not self.fields:
            self._readDataWithUnspecifiedFields(reader)
        else:
            self._readDataWithSpecifiedFields(reader)

    def _readDataWithUnspecifiedFields(self, data):
        '''Save all <data> in this table and identify its fields'''
        for dataset in data:
            row_index = dataset[self.indexField]
            self.indexedContent[row_index] = {}
            for k, v in dataset.items():
                if v is not None:
                    self.indexedContent[row_index][k] = v
                    self._addField(k, type(v))

    def _readDataWithSpecifiedFields(self, data):
        '''Save <data> of specified fields in this table'''
        for dataset in data:
            row_index = dataset[self.indexField]
            self.indexedContent[row_index] = {}
            for k, v in dataset.items():
                if v is not None and k in self.fields:
                    if isinstance(v, str) and k in self.numTypeFields:
                        value = ast.literal_eval(v)
                        assert isinstance(value, (int, float))
                        self.indexedContent[row_index][k] = value
                    else:
                        self.indexedContent[row_index][k] = v


    def toCsv(self, location):
        fieldnames = [self.indexField] + sorted(list(self.fields-set([self.indexField])))
        writer = csv.DictWriter(location,
                                fieldnames=fieldnames,
                                delimiter='|',
                                lineterminator='\n',                                
                                quoting=csv.QUOTE_NONNUMERIC,
                                quotechar='"'
        )
        writer.writeheader()
        writer.writerows(self.content)
        return location
        
if __name__ == '__main__':
    food = Table(
        indexField='id',
        content = [
            {'id': 0, 'name': 'avocado', 'type': 'fruit', 'amount': 10}
        ]
    )
    with Database('food.db') as db:
        db['food'] = food

