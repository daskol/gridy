from argparse import ArgumentParser, Namespace

parser = ArgumentParser(description=__doc__)


def main():
    ns: Namespace = parser.parse_args()
