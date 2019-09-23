from src.BaseDataTable import BaseDataTable
from src.SQLHelper import _get_default_connection, run_q, create_select, template_to_where_clause, create_insert, create_update, create_del
import pymysql

class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """


    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "key_columns": key_columns,
            }

        self.default_cnx = _get_default_connection()
        self.cur =  self.default_cnx.cursor()

    def key_to_temp(self, key_fields):
        key_col = self._data["key_columns"]
        tmp = {}
        for i, key in enumerate(key_col):
            tmp[key] = key_fields[i]

        return tmp
    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        tmp = self.key_to_temp(key_fields)
        result = self.find_by_template(tmp, field_list= field_list)

        if len(result)>1:
            print('ERROR: Primary Key is not unique')
            return

        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        selectc = create_select(self._data["table_name"], template, field_list)
        result = run_q(selectc[0],selectc[1])
        return result[1]


    def delete_by_key(self, key_fields, commit = False):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        tmp = self.key_to_temp(key_fields)
        result = self.delete_by_template(tmp, commit=commit)
        return result

    def delete_by_template(self, template, commit = False):
        """
        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        delc = create_del(self._data["table_name"], template)
        result = run_q(delc[0], delc[1], commit = commit)
        return(result[0])

    def update_by_key(self, key_fields, new_values):
        """
        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        tmp = self.key_to_temp(key_fields)
        new_values_dict  = self.key_to_temp(new_values)
        result = self.update_by_template(tmp, new_values_dict)
        return result

    def update_by_template(self, template, new_values):
        """
        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        updatec = create_update(self._data["table_name"], new_values, template)
        result = run_q(updatec[0], updatec[1])
        return result[0]

    def insert(self, new_record):
        """
        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        insertc = create_insert(self._data["table_name"], new_record)
        run_q(insertc[0], insertc[1])

    def get_rows(self):
        return self._rows
