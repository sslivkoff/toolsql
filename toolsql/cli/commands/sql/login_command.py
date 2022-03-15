import subprocess


def get_command_spec():
    return{
        'f': login_command,
        'help': '',
        'inject': ['db_config'],
    }


def login_command(db_config, **kwargs):

    if db_config['dbms'] == 'postgres':
        cmd = 'psql --dbname {database} --user {username}'
    else:
        raise NotImplementedError()
    cmd = cmd.format(**db_config)
    cmd = cmd.split(' ')
    subprocess.call(cmd)

