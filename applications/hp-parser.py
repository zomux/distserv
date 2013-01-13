import os, sys

PATH_PREPROCESS_EN = "/home/raphael/research/preprocess-en"
PATH_HP_PARSER = "/home/raphael/research/hp-parser"

def run(param):
    if 'sentence' not in param:
        return "[Error] Parameter should contain 'sentence'."
    result = os.popen("echo \"%s\" | %s | %s" % (param['sentence'], PATH_PREPROCESS_EN, PATH_HP_PARSER)).read().strip()
    return {'result':result}