from hyperopt import hp

SPACE = {
    'alpha': hp.loguniform('alpha', -3, 0),
    'gamma': hp.loguniform('gamma', -2, 0),
}


def load_search_space():
    return SPACE
