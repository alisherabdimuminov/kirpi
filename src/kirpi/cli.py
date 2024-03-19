import os
import click

database_config: str = """[DATABASE]
DB_HOST=<host>
DB_PORT=<port>
DB_NAME=<databasename>
DB_USER=<user>
DB_PASSWORD=<password>
"""

@click.command()
@click.argument("init")
def main(init) -> None:
    if init == "init":
        try:
            os.mkdir("models")
        except:
            pass
        try:
            os.mkdir("schemas")
        except:
            pass
        try:
            with open(".config", "w") as f:
                f.write(database_config)
        except:
            print("Do not create config file")
        print("Please edit .config file with your configurations.")
    else:
        raise click.BadArgumentUsage("{} is not defined".format(init))

if __name__ == "__main__":
    main()
