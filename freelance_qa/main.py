import click

from freelance_qa.cli import init_db, chat


@click.group()
def main():
    ...


main.add_command(init_db)
main.add_command(chat)