# Gridy

*Better hyperparameter searcher*

## Overview

This repository and document is created with the purpose to collect functional
and implentation requirements for a client library and server-side service for
hyperparameter optimization.

## Functional Requirements

1. Gridy must work on network file systems and NFS specifically.
2. Gridy must allow to update dynamically search space without creating new
   study (opposed to Optuna).
3. Gridy must provide fail-over mechanism. For example. workers must wait until
   budgest is exhausted or all workers fail if one worker failed.
4. Gridy must allow experiment annotation and editting.
5. Gridy must provide better prunning mechanisms (cf. @evfro).
6. Gridy must provide basic experiment management (cf. @daskol).
7. Gridy must support seeds or other common determenistic hyperparameter out of
   the box.
8. Gridy must provide persistent zero-conf storage as a default experiment
   storage facility.
