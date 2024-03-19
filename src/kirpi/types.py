import json
import re
from psycopg import sql
from psycopg.rows import class_row
from dataclasses import dataclass
from psycopg import errors

from kirpi import DataBase
from kirpi.utils import parse_filters, hash_password


class OrderBy:
    def __init__(self, *columns) -> None:
        self.columns = columns or ["id__asc"]

    def generate(self):
        order_by_columns = {}
        for i in self.columns:
            if len(i.split("__")) == 2:
                order_by_columns[i.split("__")[0]] = i.split("__")[1].upper()
        return order_by_columns

class Model:
    def __init__(self):
        self.name = self.__class__.__name__.lower() + "s"
    
    def parse_fields(self):
        fields = []
        dict_data: dict = self.__class__.__dict__
        for field in dict_data:
            if isinstance(dict_data[field], Type):
                fields.append("{} {}".format(field, dict_data[field].generate()))
        return fields
        
    def create_table(self):
        cursor = DataBase().context.cursor()
        query: sql.SQL = sql.SQL("CREATE TABLE IF NOT EXISTS {table_name} ({fields});").format(
            table_name=sql.Identifier(self.name),
            fields=sql.SQL(", ".join(self.parse_fields()))
        )
        # print(query.as_string(DataBase.context))
        cursor.execute(query=query)
        DataBase().context.commit()

        for col in self.fields():
            query: sql.SQL = sql.SQL(
                "DO $$"
                    " BEGIN"
                        " BEGIN"
                            " ALTER TABLE {} ADD COLUMN {} {};"
                        " EXCEPTION"
                            " WHEN duplicate_column THEN RAISE NOTICE 'column already exists';"
                        " END;"
                    " END;"
                " $$"
            ).format(
                sql.Identifier(self.name),
                sql.Identifier(col),
                sql.SQL(self.fields()[col].generate())
            )
            cursor.execute(query=query)
            DataBase().context.commit()
    
    def fields(self):
        fields = []
        dict_data: dict = self.__class__.__dict__
        for field in dict_data:
            if isinstance(dict_data[field], Type):
                fields.append(field)
        return fields
    
    @classmethod
    def execute(cls, query, schema: dataclass = None):
        cursor = None
        if schema:
            cursor = DataBase().context.cursor(row_factory=schema)
        else:
            cursor = DataBase().context.cursor()
        cursor.execute(query=query)
        DataBase().context.commit()
        return cursor.fet
    
    @classmethod
    def fields(cls):
        fields = {}
        dict_data: dict = cls.__dict__
        for field in dict_data:
            if isinstance(dict_data[field], Type):
                fields[field] = dict_data[field]
        return fields
    
    @classmethod
    def all(cls, page: int = 1, limit: int = 20, order_by: OrderBy = None, schema: dataclass = None):
        if order_by:
            if isinstance(order_by, OrderBy):
                pass
            else:
                order_by = OrderBy()
        else:
            order_by = OrderBy()
        page = page - 1
        fields: list = []
        if schema:
            for i in schema.__annotations__:
                if i == "id":
                    continue
                else:
                    fields.append(sql.Identifier(i))
        else:
            for i in cls.fields():
                if i == "id":
                    continue
                else:
                    fields.append(sql.Identifier(i))
        cursor = None
        if schema:
            cursor = DataBase().context.cursor(row_factory=class_row(schema))
        else:
            cursor = DataBase().context.cursor()
        query = sql.SQL("SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY id) AS \"id\", {fields} FROM {table}) x WHERE \"id\" > {page} ORDER BY {order_by} LIMIT {limit};").format(
            fields=sql.SQL(",").join(fields),
            table=sql.Identifier(cls.__name__.lower() + "s"),
            page=page,
            order_by=sql.SQL(", ".join([" ".join([i, order_by.generate()[i]]) for i in order_by.generate()])),
            limit=limit
        )
        cursor.execute(query=query)
        return cursor.fetchall()

    @classmethod
    def filter(cls, page: int = 1, limit: int = 20, order_by: OrderBy = None, schema: dataclass = None, **filters):
        if order_by:
            if isinstance(order_by, OrderBy):
                pass
            else:
                order_by = OrderBy()
        else:
            order_by = OrderBy()
        page = page - 1
        query = ""
        cursor = None
        if schema:
            cursor = DataBase().context.cursor(row_factory=class_row(schema))
        else:
            cursor = DataBase().context.cursor()
        fields: list = []
        if schema:
            for i in schema.__annotations__:
                if i == "id":
                    continue
                else:
                    fields.append(sql.Identifier(i))
        else:
            for i in cls.fields():
                if i == "id":
                    continue
                else:
                    fields.append(sql.Identifier(i))
        if parse_filters(**filters):
            query = sql.SQL("SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY id) AS \"id\", {fields} FROM {table}) x WHERE id > {page} AND {filters} ORDER BY {order_by} LIMIT {limit};").format(
                fields=sql.SQL(",").join(fields),
                table=sql.Identifier(cls.__name__.lower() + "s"),
                page=page,
                filters=sql.SQL(" AND ".join(list(map(lambda x: x.as_string(DataBase().context), parse_filters(**filters))))),
                order_by=sql.SQL(", ".join([" ".join([i, order_by.generate()[i]]) for i in order_by.generate()])),
                limit=limit
            )
        else:
            query = sql.SQL("SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY id) AS \"id\", {fields} FROM {table}) x WHERE id > {page} {filters} ORDER BY {order_by} LIMIT {limit};").format(
                fields=sql.SQL(",").join(fields),
                table=sql.Identifier(cls.__name__.lower() + "s"),
                page=page,
                filters=sql.SQL(" AND ".join(list(map(lambda x: x.as_string(DataBase().context), parse_filters(**filters))))),
                order_by=sql.SQL(", ".join([" ".join([i, order_by.generate()[i]]) for i in order_by.generate()])),
                limit=limit
            )
        cursor.execute(query=query)
        return cursor.fetchall()
    
    def delete(obj):
        cursor = DataBase().context.cursor()
        query: sql.SQL = sql.SQL("DELETE FROM {table} WHERE \"id\"={id};").format(
            table=sql.Identifier(obj.__class__.__base__().name),
            id=sql.Literal(obj.id)
        )
        cursor.execute(query=query)
        DataBase().context.commit()

    def save(obj):
        cursor = DataBase().context.cursor()
        fields = obj.__dict__
        set_: list = []
        for field in fields:
            if field == "id":
                continue
            else:
                set_.append(sql.SQL("{}={}").format(sql.Identifier(field), sql.Literal(fields[field])))
        query: sql.SQL = sql.SQL("UPDATE {} SET {} WHERE {};").format(
            sql.Identifier(obj.__class__.__base__().name), 
            sql.SQL(", ".join(list(map(lambda x: x.as_string(DataBase().context), set_)))),
            sql.SQL("id={}".format(obj.id))
        )
        cursor.execute(query=query)
        DataBase().context.commit()

    @classmethod
    def get(cls, id: int, schema: dataclass = None):
        cursor = None
        fields: list = []
        if schema:
            for field in schema.__annotations__:
                fields.append(sql.Identifier(field))
        else:
            for field in cls.fields():
                fields.append(sql.Identifier(field))
        if schema:
            cursor = DataBase().context.cursor(row_factory=class_row(schema))
        else:
            cursor = DataBase().context.cursor()
        query: sql.SQL = sql.SQL("SELECT {fields} FROM {table} WHERE id={id};").format(
            fields=sql.SQL(", ").join(fields),
            table=sql.Identifier(cls().name),
            id=id
        )
        cursor.execute(query=query)
        return cursor.fetchone()
    
    @classmethod
    def create(cls, **data):
        if data:
            cursor = DataBase().context.cursor()
            fields: list = []
            values: list = []
            for field in data:
                fields.append(sql.Identifier(field))
            for value in data:
                if isinstance(cls.fields().get(value), Password):
                    values.append(sql.Literal(hash_password(data[value])))
                else:
                    values.append(sql.Literal(data[value]))
            query: sql.SQL = sql.SQL("INSERT INTO {table}({fields}) VALUES ({values});").format(
                table=sql.Identifier(cls.__name__.lower() + "s"),
                fields=sql.SQL(", ").join(fields),
                values=sql.SQL(", ").join(values)
            )
            try:
                cursor.execute(query=query)
                DataBase().context.commit()
            except Exception as e:
                if isinstance(e, errors.NotNullViolation):
                    print("[ERROR]: {}".format(e))
                else:
                    print(e)
        else:
            print("[ERROR] no fields")

class Type:
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        self.null: bool = null
        self.unique: bool = unique
        self.primary_key: bool = primary_key
        self.is_array: bool = is_array
        self.default: str | int | float = default
        self.to_table: str = to_table
        self.to_column: str = to_column
        self.name: str = self.__class__.__name__.upper()

    def generate(self):
        params: list = []
        if self.is_array:
            self.name = self.name + "[]"
        params.append(self.name)
        if self.null:
            params.append("NULL")
        else:
            params.append("NOT NULL")
        if self.unique:
            params.append("UNIQUE")
        if self.primary_key:
            params.append("PRIMARY KEY")
        if self.default:
            re_compile = re.compile(r"\w+\(\)")
            re_match = re_compile.match(self.default)
            if re_match:
                params.append("DEFAULT {}".format(re_match.group(0)))
            elif type(self.default) == str:
                params.append("DEFAULT '{}'".format(self.default))
            elif type(self.default) == dict:
                params.append("DEFAULT '{}'".format(json.dumps(self.default)))
            else:
                params.append("DEFAULT {}".format(self.default))
        if self.to_table:
            if isinstance(self.to_table(), Model):
                if self.to_column:
                    params.append("REFERENCES {}({})".format(self.to_table().name, self.to_column))
                else:
                    params.append("REFERENCES {}(id)".format(self.to_table().name))
                    print("[WARNING]: to_column is not set, by default \"id\".")
            else:
                print("[ERROR]: to_table must be Model instance.")
        return " ".join(params)
    
class BigInt(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class BigSerial(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class Boolean(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class Date(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class Integer(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class Float(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class JSON(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class Password(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)
        self.name = "VARCHAR(255)"
    
class Real(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class SmallSerial(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class SmallInt(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class Serial(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class Text(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class Time(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class TimeStamp(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class UUID(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = "gen_random_uuid()", to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

class VarChar(Type):
    def __init__(self, max_len: int, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        self.max_len: int = max_len
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)

    def generate(self):
        params: list = []
        if self.is_array:
            self.name = self.name + "[]"
        params.append("{}({})".format(self.name, self.max_len))
        if self.null:
            params.append("NULL")
        else:
            params.append("NOT NULL")
        if self.unique:
            params.append("UNIQUE")
        if self.primary_key:
            params.append("PRIMARY KEY")
        if self.to_table:
            if isinstance(self.to_table(), str):
                if self.to_column:
                    params.append("REFERENCES {}({})".format(self.to_table().name), self.to_column)
                else:
                    params.append("REFERENCES {}(id)".format(self.to_table().name))
                    print("[WARNING]: to_column is not set, by default \"id\".")
            else:
                print("[ERROR]: to_table must be Model instance.")
        return " ".join(params)
    
class XML(Type):
    def __init__(self, null: bool = True, unique: bool = False, primary_key: bool = False, is_array: bool = False, default: str | int | float = None, to_table: None = None, to_column: None = None):
        super().__init__(null, unique, primary_key, is_array, default, to_table, to_column)
