import ast, time
import os

try:
    os.mkdir("data")
except:
    pass

try:
    os.mkdir("data/logs")
except:
    pass

title_log = f"{int(time.time() * 100000)}.txt"
def saveLog(log):
    if "connection" not in str(log).lower():
        f = open(f"data/logs/{title_log}", "a", encoding='utf-8')
        info = f"{str(log)}\n"
        f.write(info)
        f.close()

def getSettings():
    settings = {}
    try:
        f = open("data/settings.txt")
        settings = ast.literal_eval(f.read())
        f.close()
    except:
        pass

    return settings

def saveSettings(settings):
    f = open("data/settings.txt", "w")
    f.write(str(settings))
    f.close()

def saveCouples(couples):
    f = open("data/couples.txt", "w")
    f.write(str(couples))
    f.close()

def getCouples():
    settings = {}
    try:
        f = open("data/couples.txt")
        settings = ast.literal_eval(f.read())
        f.close()
    except:
        pass

    return settings

def saveAPI(API):
    f = open("data/API.txt", "w")
    f.write(str(API))
    f.close()

def getAPI():
    API = {"API_KEY": "", "SECRET_KEY": ""}
    try:
        f = open("data/API.txt")
        API = ast.literal_eval(f.read())
        f.close()
    except:
        pass

    return API