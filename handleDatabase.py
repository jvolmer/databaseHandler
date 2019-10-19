import csv
import sqlite3
import pandas as pd
import operator

# class FileMissingError(Exception):
#     def __init__(self, filename):
#         super().__init__(F'Unable to open file {filename}')

class InputError(Exception):
    def __init__(self, string):
        super().__init__(f"There are non-alphanumeric characters in '{string}' that are not allowed for identifiers in the database.")

class SQLIdentifier:
    '''Handles values, e.g. parameters, that are given to the SQLite-database'''

    def __init__(self, value) -> None:
        self.value = value

    def __repr__(self) -> str:
        '''Raise error if self.value includes any non-(alphanumerical or "_") character'''
        for char in self.value:
            if not char.isalnum() and not char == '_':
                raise InputError(self.value)
        return self.value
        # value = self.value.encode('utf-8', 'strict').decode('utf-8')
        # return '"{}"'.format(value.replace('"', '""'))
        

class Database:
    '''Interacts with the SQLite-Database'''
    def __init__(self, filename: str) -> None:
        self.connection = sqlite3.connect(filename)
        self.cursor = self.connection.cursor()

    def __del__(self) -> None:
        '''Close database'''
        self.connection.close()

    def __setitem__(self, tablename, table):
        '''Safes <table> in SQL-database with <tablename> (overwrite if table already exists)'''
        self.dropTable(tablename)
        self.createTable(tablename, table.getFields())
        for dataset in table.content:
            self.insertDatasetIntoTable(tablename, dataset)

    def __getitem__(self, tablename):
        '''Returns SQL-table with <tablename>'''
        listOfDictionaries = self.getTable(tablename)
        return Table(
            indexFieldName=self.getPrimaryKeyOfTable(tablename),
            content=listOfDictionaries
        )

    def __enter__(self):
        return self

    def __exit__(self, exceptionType, exceptionValue, traceback):
        '''Commits database changes if not exception is raised, otherwise rolls back changes'''
        if exceptionValue:
            self.connection.rollback()
            # if issubclass(exceptionType, KeyboardInterrupt):
            # print("Keyboard interrupt!")
            return False
        self.connection.commit()
        return True

    def dropTable(self, tablename):
        self.cursor.execute(f'drop table if exists {SQLIdentifier(tablename)}')

    def createTable(self, tablename, fieldtypes):
        # TODO raise error if table already exists
        fieldheader = '(' + ', '.join(repr(SQLIdentifier(field)) + ' ' + fieldtypes[field] for field in fieldtypes) + ')'
        self.cursor.execute(f'create table {SQLIdentifier(tablename)} {fieldheader}')

    def insertDatasetIntoTable(self, tablename, dataset):
        # TODO check if fields exist in db-table
        fieldInput = tuple(SQLIdentifier(field) for field in dataset)
        valueInput = '(' + ', '.join('?' for a in tuple(dataset)) + ')'
        self.cursor.execute(f'insert into {SQLIdentifier(tablename)} {fieldInput} values {valueInput}', (tuple(dataset.values())))
        
    def getTable(self, tablename):
        '''Returns full db-table as dataframe'''
        # TODO check if table exists in db
        dataframe = pd.read_sql(f'select * from {SQLIdentifier(tablename)}', self.connection)
        return dataframe.to_dict(orient='records')

    def getTableinfo(self, tablename):
        '''Returns table-information of db-table as dataframe'''
        # TODO check if table exists in db
        dataframe = pd.read_sql(f'pragma table_info({SQLIdentifier(tablename)})', self.connection)
        return dataframe.to_dict(orient='records')

    def getPrimaryKeyOfTable(self, tablename):
        '''Returns fieldname of primary key field in database-table <tablename>'''
        tableinfo = self.getTableinfo(tablename)
        for dataset in tableinfo:
            if dataset['pk'] == 1:
                return dataset['name']

            

class Table:
    def __init__(self, indexFieldName, txtTypeFields=None, numTypeFields=None, content=None):
        self.indexedContent = {}
        self.indexFieldName = indexFieldName
        self.txtTypeFields = set(txtTypeFields or ())
        self.numTypeFields = set(numTypeFields or ())
        self._read(content or [])

    @classmethod
    def copy(cls, table):
        '''Copies <table> to this Table instance'''
        return(cls(
            indexFieldName=''.join(table.indexFieldName),
            txtTypeFields={field for field in table.txtTypeFields},
            numTypeFields={field for field in table.numTypeFields},
            content=[{k: v for k, v in x.items()} for x in table.content]
        ))

    @property
    def content(self):
        '''Table content in form of a list of datasets'''
        return list(self.indexedContent.values())

    @property
    def content_fields(self):
        '''All fieldnames with content (meaning: all but key field)'''
        return self.txtTypeFields | self.numTypeFields

    @property
    def fields(self):
        '''All fieldnames'''
        return {self.indexFieldName} | self.content_fields

    def contentOf(self, keyvalue, columnname):
        '''Return content of field <columnname> in dataset with key <keyvalue> '''
        try:
            return self.indexedContent[keyvalue][columnname]
        except KeyError:
            return None

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

    def _readDataWithUnspecifiedFields(self, data):
        '''Save all <data> in this table and identify its fields'''
        for dataset in data:
            row_index = int(dataset[self.indexFieldName])
            self.indexedContent[row_index] = {}
            for k, v in dataset.items():
                if v is not None:
                    self.indexedContent[row_index][k] = v
                    self._addField(k, type(v))

    def _readDataWithSpecifiedFields(self, data):
        '''Save <data> of specified fields in this table'''
        for dataset in data:
            self.indexedContent[int(dataset[self.indexFieldName])] = {
                k: v for k, v in dataset.items()
                if k in self.fields and v is not None
            }

    def _read(self, data):
        if isinstance(data, str):
            reader = csv.DictReader(data.splitlines(), delimiter='|', quotechar='"')
        else:
            reader = data
        if not self.content_fields:
            self._readDataWithUnspecifiedFields(reader)
        else:
            self._readDataWithSpecifiedFields(reader)

    def getFields(self):
        '''Returns all fields with fieldtypes'''
        fieldtypes = {}
        for field in self.fields:
            fieldtype = 'text' if field in self.txtTypeFields else 'numeric'
            if field == self.indexFieldName:
                fieldtype += ' primary key'
            fieldtypes[field] = fieldtype
        return fieldtypes

    def __lshift__(self, table):
        '''Returns a tables that is this table, overwritte by <table>'''
        resultTable = Table.copy(self)
        for i in table.indexedContent:
            for columnToUpdate in table.content_fields:
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
                key=operator.itemgetter(self.indexFieldName)
            ) == sorted(
                table.content,
                key=operator.itemgetter(self.indexFieldName)
            )
        ) and (
            self.indexFieldName == table.indexFieldName
        ) and (
            self.txtTypeFields == table.txtTypeFields
        ) and (
            self.numTypeFields == table.numTypeFields
        )

    def __repr__(self):
        return str(self.content)


if __name__ == '__main__':
    pass
    # try:
    #     food = DatabaseUpdate(
    #         'food.csv',
    #         txtTypeFields=['name', 'type', 'family'],
    #         int_fields=[]
    #     )
    # except FileMissingError as error:
    #     print(F'Error: {error}')
    # else:
    #     with Database('food.db') as db:
    #         db['food'].update(food)



# class JunctionTable(Table):
#       def

# class ToolTable(Table):
#       def __init(name, sqlCursor)
#       def createAndFill(csvReader)
