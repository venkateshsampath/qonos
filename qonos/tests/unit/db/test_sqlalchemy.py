import qonos.db.sqlalchemy.api as db_api
from qonos.tests import utils as utils


class TestSqlalchemySpecificApi(utils.BaseTestCase):

    def test_force_dict(self):
        @db_api.force_dict
        def return_object():
            return (('foo', 'bar'), ('1', '2'))

        value = return_object()
        self.assertTrue(isinstance(value, dict))

    def test_force_dict_bad_data(self):
        @db_api.force_dict
        def return_object():
            return "I can't be a dict"

        self.assertRaises(ValueError, return_object)

    def test_force_dict_bad_list(self):
        @db_api.force_dict
        def return_object():
            return ["I can't be a dict"]

        self.assertRaises(ValueError, return_object)

    def test_force_dict_list(self):
        @db_api.force_dict
        def return_object():
            return [(('foo', 'bar'),), (('1', '2'),)]

        value = return_object()
        self.assertFalse(isinstance(value, dict))
        self.assertTrue(isinstance(value[0], dict))
        self.assertEqual(value[0].get('foo'), 'bar')
        self.assertTrue(isinstance(value[1], dict))
        self.assertEqual(value[1].get('1'), '2')
