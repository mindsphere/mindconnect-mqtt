import json

def parse(env, file_location):
    f = open(file_location, 'r')
    content = json.load(f)
    content = dict(content)
    env = str(env).upper()
    if env in content.keys():
        return content[env]
    else:
        print("Selected env does not exist!")
