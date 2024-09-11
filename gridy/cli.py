from argparse import ArgumentParser, Namespace

parser = ArgumentParser(description=__doc__)


def main():
    _: Namespace = parser.parse_args()
