# Kirpi ORM
## Lightweight and simple ORM library for Python(with psycopg)

### Installation
```bash
pip install kirpi
```
### Initialization
```bash
kirpi init
```
### Write models
```python
# models/user.py
from kirpi import types


class User(types.Model):
    id = types.Serial(primary_key=True, null=False)
    username = types.VarChar(max_len=20, unique=True)
    first_name = types.VarChar(max_len=20)
    last_name = types.VarChar(max_len=20)

User().create()
```

### Write schemas
```python
# schemas/user.py
from dataclasses import dataclass
from kirpi import Model
from models.user import User as user


@dataclass
class User(user, Model):
    id: int
    username: str
    first_name: str
    last_name: str
```

### Usage models and schemas
```python
# test.py
from schemas.user import User as UserSchema
from models.user import User

all_users = User.all(schema=UserSchema)
print(all_users)
```

### get all objects ```Model.all()```
```python
# returns list of all users on page 1
users = User.all()

# returns list of all users on page 2
users_page_2 = User.all(page=2)
```

### get objects with filter ```Model.filter()```
```python
# returns a list of all users whose first_name starts with Ali
User.filter(first_name_starts_with="Ali")
```

### get one object ```Model.get()```
```python
# returns a user whose id is equal to 1 
User.get(id=1)
```