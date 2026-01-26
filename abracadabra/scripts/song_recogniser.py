import os
import click
import abracadabra.recognise as recog
import abracadabra.register as regist
import abracadabra.storage as storage
import abracadabra.utils as utils


@click.group()
def cli():
    pass


@click.command(help="Info")
@click.argument("path")
def info(path):
    print(utils.get_song_info(path))


@click.command(
    help="Initialise the DB, needs to be done before other commands")
def initialise():
    storage.setup_db()
    click.echo("Initialised DB")


@click.command(help="Recognise a song at a filename or using the microphone")
@click.argument("path", required=False)
@click.option("--listen", is_flag=True,
              help="Use the microphone to listen for a song")
def recognise(path, listen):
    if listen:
        result = recog.listen_to_song()
        click.echo(result)
    else:
        result = recog.recognise_song(path)
        click.echo(result)


@click.command(help="Record a song from the microphone")
@click.argument("path")
def record(path):
    recog.record_audio(filename=path)


@click.command(help="Register a song or a directory of songs")
@click.argument("path")
def register(path):
    if os.path.isdir(path):
        regist.register_directory(path)
    else:
        regist.register_song(path)


@click.command(help="Get song IDs for a given album and title")
@click.argument("album")
@click.argument("title")
def get_song_ids(album, title):
    click.echo(storage.get_song_ids(album, title))


cli.add_command(get_song_ids)
cli.add_command(info)
cli.add_command(initialise)
cli.add_command(recognise)
cli.add_command(record)
cli.add_command(register)

if __name__ == "__main__":
    cli()
