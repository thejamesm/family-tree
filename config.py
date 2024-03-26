from configparser import ConfigParser

def load_config(section, filename='config.ini'):
    parser = ConfigParser()
    parser.read(filename)
    
    if parser.has_section(section):
        config = dict(parser.items(section))
    else:
        raise Exception(f'Section "{section}" not found in the file "{filename}"')

    return config

if __name__ == '__main__':
    config = load_config()
    print(config)