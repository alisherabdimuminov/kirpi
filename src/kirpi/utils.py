from psycopg import sql
from psycopg.rows import dict_row
import base64
import hashlib
import secrets
import re

from kirpi.base import DataBase


ALGORITHM = "pbkdf2_sha256"
EMAIL_REGEX = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'

def check_email(email):
    if (re.fullmatch(EMAIL_REGEX, email)):
        return True
    return False

def execute(query: str) -> tuple:
    cursor = DataBase().context.cursor(row_factory=dict_row)
    cursor.execute(query=query)
    DataBase().context.commit()
    return cursor.fetchall()

def hash_password(password: str, salt=None, iterations=260000):
    if salt is None:
        salt = secrets.token_hex(16)
    assert salt and isinstance(salt, str) and "$" not in salt
    assert isinstance(password, str)
    pw_hash = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations
    )
    b64_hash = base64.b64encode(pw_hash).decode("ascii").strip()
    return "{}${}${}${}".format(ALGORITHM, iterations, salt, b64_hash)


def verify_password(password: str, password_hash: str):
    if (password_hash or "").count("$") != 3:
        return False
    algorithm, iterations, salt, b64_hash = password_hash.split("$", 3)
    iterations = int(iterations)
    assert algorithm == ALGORITHM
    compare_hash = hash_password(password, salt, iterations)
    return secrets.compare_digest(password_hash, compare_hash)

def hash_token(number: int):
    return secrets.token_hex(number)

def parse_filters(**kwargs):
    filters = []
    field = operator = ""
    for i in kwargs:
        if len(i.split("__")) == 2:
            field, operator = i.split("__")
            if operator == "ilike":
                filters.append(sql.SQL("{} ILIKE {}").format(sql.Identifier(field), sql.Literal(kwargs[i] + "%")))
            elif operator == "like":
                filters.append(sql.SQL("{} LIKE {}").format(sql.Identifier(field), sql.Literal(kwargs[i] + "%")))
            elif operator == "starts_with":
                filters.append(sql.SQL("{} ILIKE {}").format(sql.Identifier(field), sql.Literal(kwargs[i] + "%")))
            elif operator == "ends_with":
                filters.append(sql.SQL("{} ILIKE {}").format(sql.Identifier(field), sql.Literal("%" + kwargs[i])))
            elif operator == "lt":
                filters.append(sql.SQL("{} < {}").format(sql.Identifier(field), sql.Literal(kwargs[i])))
            elif operator == "gt":
                filters.append(sql.SQL("{} > {}").format(sql.Identifier(field), sql.Literal(kwargs[i])))
            elif operator == "lte":
                filters.append(sql.SQL("{} <= {}").format(sql.Identifier(field), sql.Literal(kwargs[i])))
            elif operator == "gte":
                filters.append(sql.SQL("{} >= {}").format(sql.Identifier(field), sql.Literal(kwargs[i])))
            elif operator == "ne":
                filters.append(sql.SQL("{} <> {}").format(sql.Identifier(field), sql.Literal(kwargs[i])))
        else:
            filters.append(sql.SQL("{} = {}").format(sql.Identifier(i), sql.Literal(kwargs[i])))
    return filters