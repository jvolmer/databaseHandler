import unittest
import handleDatabase
import sqlite3
import io

def assertTableEqual(table1, table2):
    if not table1 == table2:
        raise AssertionError(f'{table1} +  !=  + {table2}')


class TestTableGetFields(unittest.TestCase):
    def testOneDataset(self):
        table = handleDatabase.Table(
            indexField='id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit', 'amount': 10}
            ]
        )
        expected = {'id': 'integer primary key', 'name': 'text', 'type': 'text', 'amount': 'numeric'}
            
        actual = table.getFields()

        self.assertCountEqual(expected, actual)

    def testTwoDatasetsWithDifferentFields(self):
        table = handleDatabase.Table(
            indexField='id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit', 'amount': 10},
                {'id': 4, 'place': 'Nicaragua', 'weight': 2.22}
            ]
        )
        expected = {'id': 'integer primary key', 'name': 'text', 'type': 'text', 'amount': 'numeric', 'place': 'text', 'weight': 'numeric'}
            
        actual = table.getFields()

        self.assertCountEqual(expected, actual)

        
class TestTableCopy(unittest.TestCase):
    def testCopy(self):
        table = handleDatabase.Table(
            indexField='id',
            content=[
                {'id': '0', 'name' : 'avocado', 'type' : 'fruit'}
            ]
        )

        table2 = handleDatabase.Table.copy(table)

        assertTableEqual(table, table2)

        
class TestTableIO(unittest.TestCase):
    def testReadOneCsvInputLine(self):
        actual = handleDatabase.Table(
            indexField='id',
            txtTypeFields = ['name', 'type'],
            numTypeFields = ['id'],
            content='"id"|"name"|"type"\n0|"avocado"|"fruit"\n'
        )

        expected = handleDatabase.Table(
            indexField='id',
            content = [
                {'id': 0, 'name' : 'avocado', 'type' : 'fruit'}
            ]
        )
        assertTableEqual(actual, expected)

    def testWriteToCsvOutput(self):
        table = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name' : 'avocado', 'type' : 'fruit'}
            ]
        )

        expected = '"id"|"name"|"type"\n0|"avocado"|"fruit"\n'

        actual = table.toCsv(io.StringIO())
        self.assertEqual(actual.getvalue(), expected)

    def testWriteAndReadCsvFile(self):

        expected = handleDatabase.Table(
            indexField='id',
            content = [
                {'id': 0, 'name' : 'avocado', 'type' : 'fruit'}
            ]
        )

        with open('csv.csv', 'w+', encoding='utf-8') as f:
            expected.toCsv(f)

        with open('csv.csv', 'r', encoding='utf-8') as f:
            actual = handleDatabase.Table(
                indexField='id',
                txtTypeFields = ['name', 'type'],
                numTypeFields = ['id'],
                content=f
            )
            
            assertTableEqual(actual, expected)


class TestTableIndexField(unittest.TestCase):
    def testCreateTextIndexField(self):
        actual = handleDatabase.Table(
            indexField = 'name',
            content = [
                {'name': 'Julia', 'age': 31}
            ]
        )

        expected = handleDatabase.Table(
            indexField = 'name',
            txtTypeFields = ['name'],
            numTypeFields = ['age'],
            content = [
                {'name': 'Julia', 'age': 31}
            ]
        )

        assertTableEqual(actual, expected)
        
    def testDoubleIndex(self):
        actual = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'},
                {'id': 0, 'name': 'kale', 'type': 'vegetable'}
            ]
        )

        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'kale', 'type': 'vegetable'}
            ]
        )

        assertTableEqual(actual, expected)
        
class TestTableCombination(unittest.TestCase):

    def test_modifyOneEntry_oneAndOneDataset(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'}
            ]
        )

        table2 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'newfruit'}
            ]
        )

        actual = table1 << table2

        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'newfruit'}
            ]
        )
        
        assertTableEqual(actual, expected)
        
    def test_modifyOneEntryWithExplicitFieldCheck_oneAndOneDataset(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit', 'number': 10}
            ]
        )

        table2 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'newfruit'}
            ]
        )

        actual = table1 << table2

        expected = handleDatabase.Table(
            indexField = 'id',
            numTypeFields = ['number', 'id'],
            txtTypeFields = ['name', 'type'],
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'newfruit', 'number': 10}
            ]
        )
        
        assertTableEqual(actual, expected)

    def test_modifyTwoEntries_oneAndOneDataset(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'}
            ]
        )
        table2 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'newavocado', 'type': 'newfruit'}
            ]
        )

        actual = table1 << table2

        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'newavocado', 'type': 'newfruit'}
            ]
        )
        
        assertTableEqual(actual, expected)

    def test_modifySpecifiedEntry_oneAndOneDataset(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'}
            ]
        )
        table2 = handleDatabase.Table(
            indexField = 'id',
            txtTypeFields = ['type'],
            numTypeFields = ['id'],
            content = [
                {'id': 0, 'name': 'newavocado', 'type': 'newfruit'}
            ]
        )
        
        actual = table1 << table2
        
        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'newfruit'}
            ]
        )

        assertTableEqual(actual, expected)

    def test_modifyOneEntries_twoAndOneDataset(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 2, 'name': 'kale', 'type': 'vegetable'},
                {'id': 0, 'name': 'avocado', 'type': 'fruit'}
            ]
        )
        table2 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'newfruit'}
            ]
        )

        actual = table1 << table2

        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 2, 'name': 'kale', 'type': 'vegetable'},
                {'id': 0, 'name': 'avocado', 'type': 'newfruit'}
            ]
        )

        assertTableEqual(actual, expected)

    def test_addOneDataset_oneAndOneDataset(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'}
            ]
        )
        table2 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 1, 'name': 'kale', 'type': 'vegetable'}
            ]
        )

        actual = table1 << table2

        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'},
                {'id': 1, 'name': 'kale', 'type': 'vegetable'}
            ]
        )

        assertTableEqual(actual, expected)

    def test_addOneDataset_twoAndTwoDatasets(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'},
                {'id': 4, 'name': 'banana', 'type': 'fruit'}
            ]
        )
        table2 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 1, 'name': 'kale', 'type': 'vegetable'},
                {'id': 7, 'name': 'salad', 'type': 'vegetable'}
            ]
        )

        actual = table1 << table2

        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'},
                {'id': 1, 'name': 'kale', 'type': 'vegetable'},
                {'id': 4, 'name': 'banana', 'type': 'fruit'},
                {'id': 7, 'name': 'salad', 'type': 'vegetable'}
            ]
        )
        
        assertTableEqual(actual, expected)

    def test_addEntry_oneAndOneDataset(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'}
            ]
        )
        table2 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit', 'color': 'green'}
            ]
        )

        actual = table1 << table2

        
        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit', 'color': 'green'}
            ]
        )        

        assertTableEqual(actual, expected)

    def test_addDatasetWithNewEntry_oneAndOneDataset(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'}
            ]
        )

        table2 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 2, 'name': 'kale', 'type': 'vegetable', 'color': 'green'}
            ]
        )
        
        actual = table1 << table2

        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'},
                {'id': 2, 'name': 'kale', 'type': 'vegetable', 'color': 'green'}
            ]
        )
        
        assertTableEqual(actual, expected)

    def test_addDatasetWithSpecifiedNewEntry_oneAndOneDataset(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'}
            ]
        )
        table2 = handleDatabase.Table(
            indexField = 'id',
            txtTypeFields = ['color'],
            numTypeFields = ['id'],
            content = [
                {'id': 2, 'name': 'kale', 'type': 'vegetable', 'color': 'green'}
            ]
        )

        actual = table1 << table2

        expected = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'},
                {'id': 2, 'color': 'green'}
            ]
        )
        
        assertTableEqual(actual, expected)
        
class TestDatabaseIO(unittest.TestCase):
    def testReadAndWriteFromAndToTable(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'name': 'avocado', 'type': 'fruit'},
                {'id': 1, 'name': 'test'}
            ]
        )

        with handleDatabase.Database('food.db') as db:
            db['food'] = table1
            table2 = db['food']

        assertTableEqual(table1, table2)

    def testWritingWithNonvalidFieldName(self):
        table1 = handleDatabase.Table(
            indexField = 'id',
            content = [
                {'id': 0, 'new type': 'fruit'}
            ]
        )

        with handleDatabase.Database('food.db') as db:
            with self.assertRaises(handleDatabase.DatabaseInputError):
                db['food'] = table1

    def testReadingFromNonexistingDBTable(self):
        with handleDatabase.Database('food.db') as db:
            with self.assertRaises(handleDatabase.DatabaseReadError):
                table = db['test']

    
class TestSQLIdentifier(unittest.TestCase):
    def testValidString(self):
        string = 'blablablaJaj_akdiepow948833dsfjdfi'

        expected = string
        actual = repr(handleDatabase.SQLIdentifier(string))

        self.assertEqual(expected, actual)

    def testInvalidString(self):
        string = 'kifeifiejfi e'

        with self.assertRaises(handleDatabase.DatabaseInputError):
            repr(handleDatabase.SQLIdentifier(string))
        
if __name__ == '__main__':
    unittest.main()
