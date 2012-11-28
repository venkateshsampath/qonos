import qonos.db.sqlalchemy.api as db_api
from qonos.tests import utils as utils


class TestSqlalchemySpecificApi(utils.BaseTestCase):

    def test_force_dict(self):
        @db_api.force_dict
        def return_object():
            return (('foo', 'bar'), ('_sa_instance_state', 'blah'))

        value = return_object()
        self.assertTrue(isinstance(value, dict))
        self.assertFalse('_sa_instance_state' in value)

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
            return [(('foo', 'bar'),), (('_sa_instance_state', 'blah'),)]

        value = return_object()
        self.assertFalse(isinstance(value, dict))
        self.assertTrue(isinstance(value[0], dict))
        self.assertEqual(value[0].get('foo'), 'bar')
        self.assertFalse('_sa_instance_state' in value[1])
