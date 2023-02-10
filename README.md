# Scheduler Tools

Basic tools for automating some PBS work. In progress, and potentially unsafe.

## Usage

Recommended: Run the provided tools as services (with the `-s` flag), in a designated environment.

```
conda create -n schedtools python=3.8
conda activate schedtools
pip install -e .
```

This should allow the service to access the correct Python installation upon repeated reboots, and ensures that
the environment is stable.