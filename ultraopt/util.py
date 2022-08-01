from hyperopt import hp

SPACE = {
    'alpha': hp.loguniform('alpha', -3, 0),
    'gamma': hp.loguniform('gamma', -2, 0),
}


def load_objective_func():
    def objective(*, alpha, gamma):
        return alpha ** 2 + gamma ** 2
    return objective


def load_search_space():
    return SPACE
