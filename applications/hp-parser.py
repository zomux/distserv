import os, sys

PREPROCESS_EN = "python /home/raphael/research/preprocess-en/preprocess-line.py"
HP_PARSER = "python /home/raphael/research/hp-parser/hp-parser.py"

def run(param):
    if 'sentence' not in param:
        return "[Error] Parameter should contain 'sentence'."
    result = os.popen("echo \"%s\" | %s | %s" % (param['sentence'], PREPROCESS_EN, HP_PARSER)).read().strip()
    return {'result':result}