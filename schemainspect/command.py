import argparse
import pgpasslib
import sys
import yaml
import getpass
from sqlalchemy.engine.url import make_url
from io import StringIO as sio
from sqlbag import S

from .get import get_inspector
from .misc import quoted_identifier
from .tableformat import t


def parse_args(args):
    parser = argparse.ArgumentParser(description="Inspect a schema")

    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    parser_deps = subparsers.add_parser("deps", help="Show inspected dependencies")
    parser_deps.add_argument("db_url", help="URL")

    parser_deps2 = subparsers.add_parser(
        "yaml", help="Export schema definition as YAML"
    )
    parser_deps2.add_argument("db_url", help="URL")
    parser_deps2.add_argument("--include", help="Include schemas", nargs="*")
    parser_deps2.add_argument("--exclude", help="Exclude schemas", nargs="*")

    return parser.parse_args(args)

def get_password_from_pgpass(host, port, database, username):
    if not host:
        host = 'localhost'

    if not port:
        port = 5432

    if not username:
        username = getpass.getuser()

    if not database:
        database = username

    try:
        return pgpasslib.getpass(
            host,
            port,
            database,
            username,
        )
    except pgpasslib.FileNotFound:
        return None

    return None


def get_password_from_uri(uri):
    uri = make_url(uri)

    if not uri.password:
        password = get_password_from_pgpass(
            uri.host,
            uri.port,
            uri.database,
            uri.username
        )
        if password:
            uri.set(password=password)

    return uri

def do_deps(db_url):
    with S(db_url) as s:
        i = get_inspector(s)
        deps = i.deps

    def process_row(dep):
        depends_on = quoted_identifier(dep.name, dep.schema, dep.identity_arguments)
        thing = quoted_identifier(
            dep.name_dependent_on,
            dep.schema_dependent_on,
            dep.identity_arguments_dependent_on,
        )

        return dict(
            thing="{}: {}".format(dep.kind_dependent_on, thing),
            depends_on="{}: {}".format(dep.kind, depends_on),
        )

    deps = [process_row(_) for _ in deps]
    rows = t(deps)

    if rows:
        print(rows)
    else:
        print("No dependencies found.")


def do_yaml(db_url, include_schemas, exclude_schemas):
    with S(db_url) as s:
        i = get_inspector(s, include_schemas, exclude_schemas)
        defn = i.encodeable_definition()

    x = sio()
    yaml.safe_dump(defn, x)
    print(x.getvalue())


def run(args):
    db_url = get_password_from_uri(args.db_url)

    if args.command == "deps":
        do_deps(db_url)

    elif args.command == "yaml":
        do_yaml(db_url, args.include, args.exclude)

    else:
        raise ValueError("no such command")


def do_command():  # pragma: no cover
    args = parse_args(sys.argv[1:])
    status = run(args)
    sys.exit(status)
